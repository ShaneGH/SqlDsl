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
    class SqlSelectStatement : ISqlSelectStatement
    {
        /// <inheritdoc />
        public IEnumerable<(string mappedPropertyName, ISelectColumn rowNumberColumn)> MappedPropertiesToRowNumbers { get; }

        /// <inheritdoc />
        public ISelectColumns SelectColumns { get; }

        public SqlSelectStatement(IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables, ISelectColumn primaryTableRowId)
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
            var rids = properties
                // get all PK columns from all columns from all select parts
                .SelectMany(p => p.FromParams.GetEnumerable1())
                .Where(p => !p.IsParameter)
                .Select(p => p.RowIdColumn)
                // concat with all tables which might not have explicit selects
                // (e.g. tables which are mapped like: q.Tab.Select(x => 5)
                .Concat(tables.Where(t => !t.TableResultsAreAggregated).Select(t => t.From.RowNumberColumn))
                // ensure that the primaryTableRowId comes first
                .Prepend(primaryTableRowId)
                .Distinct();

            foreach (var rid in FillOutRIDSelectColumns(rids))
                yield return rid;
                
            foreach (var prop in properties)
                yield return new SqlSelectColumn(prop);
        }

        static IEnumerable<ISelectColumn> FillOutRIDSelectColumns(IEnumerable<ISelectColumn> cols)
        {
            var cs = cols.ToList();
            for (var i = 0; i < cs.Count; i++)
                AddParentRowIdIfAvailable(cs[i]);

            void AddParentRowIdIfAvailable(ISelectColumn col)
            {
                if (col.Table?.JoinedFrom != null)
                    cs.Add(col.Table.JoinedFrom.RowNumberColumn);
            }

            return cs.Distinct();
        }
    }

    class SqlSelectColumn : ISelectColumn
    {
        public IQueryTable Table { get; }

        public string Alias { get; }

        public bool IsRowNumber  { get; }

        public Type DataType { get; }

        public ConstructorInfo[] ArgConstructors { get; }

        public bool IsAggregated { get; }

        public SqlSelectColumn(QueryElementBasedMappedProperty prop)
            : this(
                TryGetQueryTable(prop.FromParams
                    .GetEnumerable1()
                    .Where(p => !p.IsParameter)
                    .Select(GetColumn)),
                prop.To,
                false,
                IsAggregatedColumn(prop.FromParams),
                prop.MappedPropertyType,
                prop.PropertySegmentConstructors)
        {
        }

        public SqlSelectColumn(IQueryTable table, string alias, bool isRowNumber, bool isAggregated, Type dataType, ConstructorInfo[] argConstructors)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
            ArgConstructors = argConstructors ?? throw new ArgumentNullException(nameof(argConstructors));
            DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
            Table = table;
            IsRowNumber = isRowNumber;
            IsAggregated = isAggregated;
        }

        static bool IsAggregatedColumn(IAccumulator<TheAmazingElement> prop)
        {
            return prop.GetEnumerable1().All(p => p.IsParameter || p.ColumnIsAggregatedToDifferentTable);
        }

        static IQueryTable TryGetQueryTable(IEnumerable<ISelectColumn> columns)
        {
            return columns
                .Select(GetTable)
                .RemoveNulls()
                .OrderByDescending(Identity, OrderTablesByPrecedence)
                .FirstOrDefault();
        }

        static readonly Func<TheAmazingElement, ISelectColumn> GetColumn = x => x.Column;

        static readonly Func<ISelectColumn, IQueryTable> GetTable = x => x.Table;

        static readonly Func<IQueryTable, IQueryTable> Identity = x => x;

        static readonly IComparer<IQueryTable> OrderTablesByPrecedence = new TablePrecedenceOrderer();

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