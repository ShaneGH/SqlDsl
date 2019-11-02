using SqlDsl.DataParser.DataRow;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Represents an object with it's property names
    /// </summary>
    public class ObjectPropertyGraph
    {
        /// <summary>
        /// Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// Type can be null if the property is generated by sql, (e.g. if property is #pk)
        /// </summary>
        public readonly SimpleProp[] SimpleProps;
        
        /// <summary>
        /// Properties of an object which have sub properies
        /// </summary>
        public readonly ComplexProp[] ComplexProps;

        /// <summary>
        /// Constructor args of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly SimpleConstructorArg[] SimpleConstructorArgs;
        
        /// <summary>
        /// Constructor args of an object which have sub properies
        /// </summary>
        public readonly ComplexConstructorArg[] ComplexConstructorArgs;

        /// <summary>
        /// Constructor args of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly Type[] ConstructorArgTypes;
        
        /// <summary>
        /// A composite of the primary keys which point to this object
        /// </summary>
        public readonly int[] PrimaryKeyColumns;
        // if PrimaryKeyColumns are invalid
        // check mapped tables in QueryMapper.BuildMapForSelect(...)
        
        /// <summary>
        /// The type of the object
        /// </summary>
        public readonly Type ObjectType;

        private readonly IDataRowKeyComparer KeyComparer;

        private static readonly Func<IDataRow, IDataRow> ReturnInput = x => x;
        
        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="objectType">The type of the object.</param>
        /// <param name="simpleProps">Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.</param>
        /// <param name="complexProps">Properties of an object which have sub properies</param>
        /// <param name="primaryKeyColumns">A composite of the row numbers which point to this object</param>
        public ObjectPropertyGraph(
            Type objectType,
            SimpleProp[] simpleProps, 
            ComplexProp[] complexProps, 
            int[] primaryKeyColumns,
            SimpleConstructorArg[] simpleConstructorArgs = null,
            ComplexConstructorArg[] complexConstructorArgs = null)
        {
            ObjectType = objectType ?? throw new ArgumentNullException(nameof(objectType));
            SimpleProps = simpleProps ?? Array.Empty<SimpleProp>();
            ComplexProps = complexProps ?? Array.Empty<ComplexProp>();
            PrimaryKeyColumns = primaryKeyColumns ?? CodingConstants.Empty.Int;
            SimpleConstructorArgs = simpleConstructorArgs ?? Array.Empty<SimpleConstructorArg>();
            ComplexConstructorArgs = complexConstructorArgs ?? Array.Empty<ComplexConstructorArg>();
            ConstructorArgTypes = CompileConstructorArgTypes(SimpleConstructorArgs, ComplexConstructorArgs).ToArray();
            KeyComparer = new IDataRowKeyComparer(PrimaryKeyColumns);
        }

        /// <summary>
        /// Build constructor arg types and validate that all args are present
        /// </summary>
        static IEnumerable<Type> CompileConstructorArgTypes(
            IEnumerable<SimpleConstructorArg> simpleConstructorArgs,
            IEnumerable<ComplexConstructorArg> complexConstructorArgs)
        {
            var args = simpleConstructorArgs
                .Select(a => (argIndex: a.ArgIndex, a.ResultPropertyType))
                .Concat(complexConstructorArgs
                    .Select(a => (argIndex: a.ArgIndex, a.ConstuctorArgType)))
                .OrderBy(a => a.argIndex);

            var i = 0;
            foreach (var arg in args)
            {
                if (arg.argIndex != i)
                    throw new InvalidOperationException($"Expecting arg with index of {i}, but got {arg.argIndex}.");

                i++;
                yield return arg.Item2;   
            }
        }

        /// <summary>
        /// Group the data into individual objects, where an object has multiple rows (for sub properties which are enumerable).
        /// Also, remove invalid data which might have been returned as part of an outer join
        /// </summary>
        public IEnumerable<IGrouping<IDataRow, IDataRow>> GroupAndFilterData(IEnumerable<IDataRow> rows)
        {
            return rows
                .GroupBy(ReturnInput, KeyComparer)
                .Where(AllKeysHaveValue);

            bool AllKeysHaveValue(IGrouping<IDataRow, IDataRow> row) =>
                KeyComparer.AllKeysHaveValue(row.Key);
        }

        private class IDataRowKeyComparer : IEqualityComparer<IDataRow>
        {
            private int[] _keys;

            public IDataRowKeyComparer(int[] keys)
            {
                _keys = keys;
            }

            public bool AllKeysHaveValue(IDataRow row)
            {
                for (var i = 0; i < _keys.Length; i++)
                {
                    if (!row.HasValue(_keys[i]))
                        return false;
                }

                return true;
            }

            public bool Equals(IDataRow x, IDataRow y)
            {
                if (x == y)
                    return true;

                if (x == null || y == null)
                    return false;

                for (var i = 0; i < _keys.Length; i++)
                {
                    if (!x.ValueIsEqual(y, _keys[i]))
                        return false;
                }

                return true;        
            }

            // TODO: a GetHashCodeOfFirstColumn function would do well here
            // as first column is always the first primary key value
            public int GetHashCode(IDataRow obj) => 1;
        }

        /// <summary>
        /// Get the unique id of a row, in the context of a simple prop of this object
        /// </summary>
        string GetUniqueIdForSimpleProp(object[] row, IEnumerable<int> simplePropPrimaryKeyColumns)
        {
            // TODO: this method is used a lot.
            // Can results be cached or more efficient method used?
            return PrimaryKeyColumns
                .Concat(simplePropPrimaryKeyColumns.OrEmpty())
                .Select(r => row[r].ToString())
                .JoinString(";");
        }

        /// <summary>
        /// Get the rows for a simple property, given it's row number column ids
        /// </summary>
        public IEnumerable<IDataRow> GetDataRowsForSimpleProperty(IEnumerable<IDataRow> rows, int[] simplePropPrimaryKeyColumns)
        {
            var dataIsFromSameTableAsObjectContext = simplePropPrimaryKeyColumns.Length > 0;

            // if there is only 1 row, it may have null primary keys
            // in the case of an OUTER JOIN, however it is still a valid row
            var rs = rows.ToArray();
            if (rs.Length == 1 && !dataIsFromSameTableAsObjectContext)
                return rs[0].ToEnumerable();

            // run a "Distinct" on the primary keys
            return rs
                // remove empty data created by OUTER JOINs
                .Where(r => simplePropPrimaryKeyColumns.All(x => r.ToObj()[x] != null && r.ToObj()[x] != DBNull.Value))
                .GroupBy(d => GetUniqueIdForSimpleProp(d.ToObj(), simplePropPrimaryKeyColumns))
                .Select(Enumerable.First);
        }

        public override string ToString()
        {
            var primaryKeys = $"PrimaryKeyColumns: [{PrimaryKeyColumns.JoinString(",")}]";

            var simpleProps = SimpleProps
                .Select(p => $"{p.Name}: {{ index: {p.Index}, rids: [{p.PrimaryKeyColumns.JoinString(",")}], resultPropertyType: {p.ResultPropertyType?.Name ?? "null"}, dataCellType: {p.DataCellType?.Name ?? "null"} }}");

            var complexProps = ComplexProps.Select(p => $"{p.Name}:\n  {p.Value.ToString().Replace("\n", "\n  ")}");

            var simpleConstructorArgs = SimpleConstructorArgs
                .Select(p => $"CArg_{p.ArgIndex}: {{ index: {p.Index}, rids: [{p.PrimaryKeyColumns.JoinString(",")}], resultPropertyType: {p.ResultPropertyType?.Name ?? "null"}, dataCellType: {p.DataCellType?.Name ?? "null"} }}");

            var complexConstructorArgs = ComplexConstructorArgs.Select(p => $"CArg_{p.ArgIndex}:\n  {p.Value.ToString().Replace("\n", "\n  ")}");

            return new [] { ObjectType.FullName, primaryKeys }
                .Concat(simpleConstructorArgs)
                .Concat(complexConstructorArgs)
                .Concat(simpleProps)
                .Concat(complexProps)
                .JoinString("\n");
        }
    }
}