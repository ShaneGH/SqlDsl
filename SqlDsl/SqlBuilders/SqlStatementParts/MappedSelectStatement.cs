using SqlDsl.Mapper;
using SqlDsl.Query;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    /// <summary>
    /// A minimal select statement
    /// </summary>
    class MappedSelectStatement : ISqlSelectStatement
    {
        /// <inheritdoc />
        public IEnumerable<(string mappedPropertyName, ISelectColumn rowNumberColumn)> MappedPropertiesToRowNumbers { get; }

        /// <inheritdoc />
        public ISelectColumns SelectColumns { get; }

        public MappedSelectStatement(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ISelectColumn primaryTableRowId)
        {
            SelectColumns = new SqlSelectColumns(properties, tables, primaryTableRowId);
            MappedPropertiesToRowNumbers = GetMappedPropertiesToRowNumbers(tables).Enumerate();
        }

        /// <summary>
        /// Get a list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        static IEnumerable<(string mappedPropertyName, ISelectColumn rowNumberColumn)> GetMappedPropertiesToRowNumbers(IEnumerable<StrongMappedTable> tables)
        {
            return tables
                // if mapping does not map to a specific property (e.g. q => q.Args.Select(a => new object()))
                // To will be null
                .Where(t => !t.TableResultsAreAggregated)

                // TODO: setting To to "" is wishful thinking, but no
                // tests failing right now
                .Select(t => (t.To ?? "", t.From.RowNumberColumn))
                .Enumerate();
        }
    }

    class SqlSelectColumns : ISelectColumns
    {
        readonly IEnumerable<ISelectColumn> Columns;

        public SqlSelectColumns(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ISelectColumn primaryTableRowId)
        {
            Columns = BuildColumns(properties, tables, primaryTableRowId).Enumerate();
        }

        public ISelectColumn this[string alias] => TryGetColumn(alias) ??
            throw new InvalidOperationException($"There is no column with alias: \"{alias}\".");

        public IEnumerator<ISelectColumn> GetEnumerator() => Columns.GetEnumerator();

        public ISelectColumn TryGetColumn(string alias)
        {
            foreach (var col in Columns)
                if (col.Alias == alias)
                    return col;

            return null;
        }

        IEnumerator IEnumerable.GetEnumerator() => (Columns as IEnumerable).GetEnumerator();

        static IEnumerable<ISelectColumn> BuildColumns(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ISelectColumn primaryTableRowId)
        {
            var ridsForEachColumn = properties
                .Select(TryCombineWithRowNumberColumn)
                .ToArray();

            var rids = ridsForEachColumn
                .Select(x => x.Item2)
                .RemoveNulls()
                .Concat(tables
                    .Where(t => !t.TableResultsAreAggregated)
                    .Select(t => t.From.RowNumberColumn))
                .Prepend(primaryTableRowId)
                .Distinct();

            foreach (var rid in FillOutRIDSelectColumns(rids))
                yield return rid;
                
            foreach (var prop in ridsForEachColumn)
                yield return new SqlSelectColumn(prop.Item1, prop.Item2 ?? primaryTableRowId);
        }

        static IEnumerable<ISelectColumn> FillOutRIDSelectColumns(IEnumerable<ISelectColumn> cols)
        {
            return Execute(cols).Distinct();

            IEnumerable<ISelectColumn> Execute(IEnumerable<ISelectColumn> columns)
            {
                columns = columns.Enumerate();
                if (!columns.Any())
                    return Enumerable.Empty<ISelectColumn>();

                var head = columns.First();
                if (head.IsRowNumberForTable == null)
                    return Execute(columns.Skip(1)).Prepend(head);

                return FillOutJoins(head.IsRowNumberForTable)
                    .Select(x => x.RowNumberColumn)
                    .Concat(Execute(columns.Skip(1)));
            }

            IEnumerable<IQueryTable> FillOutJoins(IQueryTable t)
            {
                return t.JoinedFrom
                    .Select(FillOutJoins)
                    .SelectMany()
                    .Append(t);
            }
        }

        static readonly Func<QueryElementBasedMappedProperty, (QueryElementBasedMappedProperty, ISelectColumn)> TryCombineWithRowNumberColumn = singleSelectPart =>
        {
            return (singleSelectPart, TryGetRowNumberColumn(singleSelectPart));
        };

        static ISelectColumn TryGetRowNumberColumn(QueryElementBasedMappedProperty singleSelectPart)
        {
            return singleSelectPart.FromParams
                .GetAggregatedEnumerable()
                .Where(FilterOutAggregated)
                .Select(SelectElement)
                .Select(GetRowIdColumn)
                .Select(TryGetTable)
                .RemoveNulls()
                .OrderByDescending(Identity, new TablePrecedenceOrderer(
                    TablePrecedenceOrderer.GetSingleMappingContext(singleSelectPart)))
                .Select(TryGetRowNumberColumnFromTable)
                .FirstOrDefault();
        }

        static readonly Func<(bool isAggregated, SelectColumnBasedElement), bool> FilterOutAggregated = x => !x.isAggregated;

        static readonly Func<(bool, SelectColumnBasedElement element), SelectColumnBasedElement> SelectElement = x => x.element;

        static readonly Func<SelectColumnBasedElement, ISelectColumn> GetRowIdColumn = x => x.RowIdColumn;

        static readonly Func<IQueryTable, ISelectColumn> TryGetRowNumberColumnFromTable = x => x.RowNumberColumn;

        static readonly Func<ISelectColumn, IQueryTable> TryGetTable = x => x.IsRowNumberForTable;

        static readonly Func<IQueryTable, IQueryTable> Identity = x => x;

        class SqlSelectColumn : ISelectColumn
        {
            /// <inheritdoc />
            public string Alias { get; }

            /// <inheritdoc />
            public Type DataType { get; }

            /// <inheritdoc />
            public ConstructorInfo[] ArgConstructors { get; }

            /// <inheritdoc />
            public ISelectColumn RowNumberColumn { get; }

            /// <inheritdoc />
            public IQueryTable IsRowNumberForTable => null;

            /// <inheritdoc />
            public bool IsRowNumber => false;

            /// <inheritdoc />
            public IEnumerable<IQueryTable> MappingContext { get; }

            public SqlSelectColumn(QueryElementBasedMappedProperty prop, ISelectColumn rowIdSelectColumn)
                : this(
                    prop.To,
                    prop.MappedPropertyType,
                    prop.PropertySegmentConstructors,
                    rowIdSelectColumn,
                    prop.MappingContext)
            {
            }

            public SqlSelectColumn(string alias, Type dataType, ConstructorInfo[] argConstructors, ISelectColumn rowNumberColumn, IEnumerable<IQueryTable> mappingContext)
            {
                Alias = alias ?? throw new ArgumentNullException(nameof(alias));
                ArgConstructors = argConstructors ?? throw new ArgumentNullException(nameof(argConstructors));
                DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
                RowNumberColumn = rowNumberColumn ?? throw new ArgumentNullException(nameof(rowNumberColumn));
                MappingContext = mappingContext ?? throw new ArgumentNullException(nameof(mappingContext));
            }
        }
    }
}