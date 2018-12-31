using SqlDsl.Mapper;
using SqlDsl.Query;
using SqlDsl.Utils;
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
                .Where(t => t.To != null && !t.TableResultsAreAggregated)
                .Select(t => (resultClassProperty: t.To, t.From.RowNumberColumn))
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
                .Concat(tables.Where(t => !t.TableResultsAreAggregated).Select(t => t.From.RowNumberColumn))
                .Prepend(primaryTableRowId)
                .Distinct();

            foreach (var rid in FillOutRIDSelectColumns(rids))
                yield return rid;
                
            foreach (var prop in ridsForEachColumn)
                yield return new SqlSelectColumn(prop.Item1, prop.Item2 ?? primaryTableRowId);
        }

        static IEnumerable<ISelectColumn> FillOutRIDSelectColumns(IEnumerable<ISelectColumn> cols)
        {
            var cs = cols.ToList();
            for (var i = 0; i < cs.Count; i++)
                AddParentRowIdIfAvailable(cs[i]);

            void AddParentRowIdIfAvailable(ISelectColumn col)
            {
                if (col?.IsRowNumberForTable?.JoinedFrom != null)
                    cs.Add(col.IsRowNumberForTable.JoinedFrom.RowNumberColumn);
            }

            return cs.Distinct();
        }

        static readonly Func<QueryElementBasedMappedProperty, (QueryElementBasedMappedProperty, ISelectColumn)> TryCombineWithRowNumberColumn = singleSelectPart =>
        {
            return (singleSelectPart, TryGetRowNumberColumn(singleSelectPart));
        };

        static readonly Func<QueryElementBasedMappedProperty, ISelectColumn> TryGetRowNumberColumn = singleSelectPart =>
        {
            return singleSelectPart.FromParams
                .GetEnumerable1()
                .Select(GetRowIdColumn)
                .Select(TryGetTable)
                .RemoveNulls()
                .OrderByDescending(Identity, OrderTablesByPrecedence)
                .Select(TryGetRowNumberColumnFromTable)
                .FirstOrDefault();
        };

        static readonly Func<SelectColumnBasedElement, ISelectColumn> GetRowIdColumn = x => x.RowIdColumn;

        static readonly Func<IQueryTable, ISelectColumn> TryGetRowNumberColumnFromTable = x => x.RowNumberColumn;

        static readonly Func<ISelectColumn, IQueryTable> TryGetTable = x => x.IsRowNumberForTable;

        static readonly Func<IQueryTable, IQueryTable> Identity = x => x;

        static readonly IComparer<IQueryTable> OrderTablesByPrecedence = new TablePrecedenceOrderer();

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

            public SqlSelectColumn(QueryElementBasedMappedProperty prop, ISelectColumn rowIdSelectColumn)
                : this(
                    prop.To,
                    prop.MappedPropertyType,
                    prop.PropertySegmentConstructors,
                    rowIdSelectColumn)
            {
            }

            public SqlSelectColumn(string alias, Type dataType, ConstructorInfo[] argConstructors, ISelectColumn rowNumberColumn)
            {
                Alias = alias ?? throw new ArgumentNullException(nameof(alias));
                ArgConstructors = argConstructors ?? throw new ArgumentNullException(nameof(argConstructors));
                DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
                RowNumberColumn = rowNumberColumn ?? throw new ArgumentNullException(nameof(rowNumberColumn));
            }
        }

        class TablePrecedenceOrderer : IComparer<IQueryTable>
        {
            public int Compare(IQueryTable x, IQueryTable y)
            {
                return CompareRec(x, y) 
                    ?? throw new InvalidOperationException($"Cannot find table precendence between {x.Alias} and {y.Alias}");
            }

            int? CompareRec(IQueryTable x, IQueryTable y)
            {
                if (x == null || y == null)
                    throw new NotSupportedException();

                if (x == y) return 0;

                if (x.JoinedFrom != null)
                {
                    var i = CompareRec(x.JoinedFrom, y);
                    if (i != null)
                        return i == 0 ? 1 : i;
                }
                
                if (y.JoinedFrom != null)
                {
                    var i = CompareRec(x, y.JoinedFrom);
                    if (i != null)
                        return i == 0 ? -1 : i;
                }

                return null;
            }
        }
    }
}