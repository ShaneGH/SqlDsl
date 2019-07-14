using SqlDsl.Mapper;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    /// <summary>
    /// A minimal select statement
    /// </summary>
    class MappedSelectStatement : ISqlSelectStatement
    {
        /// <inheritdoc />
        public IEnumerable<(string mappedPropertyName, ICompositeKey primaryKey)> MappedPropertiesToPrimaryKeys { get; }

        /// <inheritdoc />
        public ISelectColumns SelectColumns { get; }

        public MappedSelectStatement(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ICompositeKey primaryTableKey)
        {
            SelectColumns = new SqlSelectColumns(properties, tables, primaryTableKey);
            MappedPropertiesToPrimaryKeys = GetMappedPropertiesToRowNumbers(tables).Enumerate();
        }

        /// <summary>
        /// Get a list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        static IEnumerable<(string mappedPropertyName, ICompositeKey primaryKey)> GetMappedPropertiesToRowNumbers(IEnumerable<StrongMappedTable> tables)
        {
            return tables
                // if mapping does not map to a specific property (e.g. q => q.Args.Select(a => new object()))
                // To will be null
                .Where(t => !t.TableResultsAreAggregated)

                // TODO: setting To to "" is wishful thinking, but no
                // tests failing right now
                .Select(t => (t.To ?? "", t.From.PrimaryKey))
                .Enumerate();
        }
    }

    class SqlSelectColumns : ISelectColumns
    {
        readonly IEnumerable<ISelectColumn> Columns;

        public SqlSelectColumns(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ICompositeKey primaryTableKey)
        {
            Columns = BuildColumns(properties, tables, primaryTableKey).Enumerate();
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

        static IEnumerable<ISelectColumn> BuildColumns(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ICompositeKey primaryTableKey)
        {
            var ridsForEachColumn = properties
                .Select(TryCombineWithRowNumberColumn)
                .ToArray();

            var rids = ridsForEachColumn
                .Select(x => x.Item2)
                .RemoveNulls()
                .Concat(tables
                    .Where(t => !t.TableResultsAreAggregated)
                    .Select(t => t.From.PrimaryKey))
                .Prepend(primaryTableKey)
                .Distinct();

            foreach (var rid in rids.FillOutRIDSelectColumns().SelectMany())
                yield return rid;
                
            foreach (var prop in ridsForEachColumn)
                yield return new SqlSelectColumn(prop.Item1, prop.Item2 ?? primaryTableKey);
        }

        static readonly Func<QueryElementBasedMappedProperty, (QueryElementBasedMappedProperty, ICompositeKey)> TryCombineWithRowNumberColumn = singleSelectPart =>
        {
            return (singleSelectPart, TryGetPrimaryKey(singleSelectPart));
        };

        static ICompositeKey TryGetPrimaryKey(QueryElementBasedMappedProperty singleSelectPart)
        {
            return singleSelectPart.FromParams
                .GetAggregatedEnumerable()
                .Where(FilterOutAggregated)
                .Select(SelectElement)
                .Select(GetRowIdColumn)
                .Select(GetTable)
                .RemoveNulls()
                .OrderByDescending(Identity, new TablePrecedenceOrderer(
                    TablePrecedenceOrderer.GetSingleMappingContext(singleSelectPart)))
                .Select(TryGetRowNumberColumnFromTable)
                .FirstOrDefault();
        }

        static readonly Func<(bool isAggregated, SelectColumnBasedElement), bool> FilterOutAggregated = x => !x.isAggregated;

        static readonly Func<(bool, SelectColumnBasedElement element), SelectColumnBasedElement> SelectElement = x => x.element;

        static readonly Func<SelectColumnBasedElement, ICompositeKey> GetRowIdColumn = x => x.PrimaryKey;

        static readonly Func<IQueryTable, ICompositeKey> TryGetRowNumberColumnFromTable = x => x.PrimaryKey;

        static readonly Func<ICompositeKey, IQueryTable> GetTable = x => x.Table;

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
            public ICompositeKey PrimaryKey { get; }

            /// <inheritdoc />
            public IQueryTable IsRowNumberForTable => null;

            /// <inheritdoc />
            public bool IsRowNumber => false;

            /// <inheritdoc />
            public IEnumerable<IQueryTable> MappingContext { get; }

            public SqlSelectColumn(QueryElementBasedMappedProperty prop, ICompositeKey columnKey)
                : this(
                    prop.To,
                    prop.MappedPropertyType,
                    prop.PropertySegmentConstructors,
                    columnKey,
                    prop.MappingContext)
            {
            }

            public SqlSelectColumn(string alias, Type dataType, ConstructorInfo[] argConstructors, ICompositeKey columnKey, IEnumerable<IQueryTable> mappingContext)
            {
                Alias = alias ?? throw new ArgumentNullException(nameof(alias));
                ArgConstructors = argConstructors ?? throw new ArgumentNullException(nameof(argConstructors));
                DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
                PrimaryKey = columnKey ?? throw new ArgumentNullException(nameof(columnKey));
                MappingContext = mappingContext ?? throw new ArgumentNullException(nameof(mappingContext));
            }
        }
    }
}