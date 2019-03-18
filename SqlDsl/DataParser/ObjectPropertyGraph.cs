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
        public static readonly (int, string, int[], Type, Type)[] EmptySimpleProps = new (int, string, int[], Type, Type)[0];
        public static readonly (string, ObjectPropertyGraph)[] EmptyComplexProps = new (string, ObjectPropertyGraph)[0];
        public static readonly (int, int, int[], Type, Type)[] EmptySimpleConstructorArgs = new (int, int, int[], Type, Type)[0];
        public static readonly (int, Type, ObjectPropertyGraph)[] EmptyComplexConstructorArgs = new (int, Type, ObjectPropertyGraph)[0];

        /// <summary>
        /// Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// Type can be null if the property is generated by sql, (e.g. if property is #rowid)
        /// </summary>
        public readonly (int index, string name, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)[] SimpleProps;
        
        /// <summary>
        /// Properties of an object which have sub properies
        /// </summary>
        public readonly (string name, ObjectPropertyGraph value)[] ComplexProps;

        /// <summary>
        /// Constructor args of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly (int index, int argIndex, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)[] SimpleConstructorArgs;
        
        /// <summary>
        /// Constructor args of an object which have sub properies
        /// </summary>
        public readonly (int argIndex, Type constuctorArgType, ObjectPropertyGraph value)[] ComplexConstructorArgs;

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
        
        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="objectType">The type of the object.</param>
        /// <param name="simpleProps">Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.</param>
        /// <param name="complexProps">Properties of an object which have sub properies</param>
        /// <param name="primaryKeyColumns">A composite of the row numbers which point to this object</param>
        public ObjectPropertyGraph(
            Type objectType,
            (int index, string name, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)[] simpleProps, 
            (string name, ObjectPropertyGraph value)[] complexProps, 
            int[] primaryKeyColumns,
            (int index, int argIndex, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)[] simpleConstructorArgs = null,
            (int argIndex, Type constuctorArgType, ObjectPropertyGraph value)[] complexConstructorArgs = null)
        {
            ObjectType = objectType ?? throw new ArgumentNullException(nameof(objectType));
            SimpleProps = simpleProps ?? EmptySimpleProps;
            ComplexProps = complexProps ?? EmptyComplexProps;
            PrimaryKeyColumns = primaryKeyColumns ?? CodingConstants.Empty.Int;
            SimpleConstructorArgs = simpleConstructorArgs ?? EmptySimpleConstructorArgs;
            ComplexConstructorArgs = complexConstructorArgs ?? EmptyComplexConstructorArgs;
            ConstructorArgTypes = CompileConstructorArgTypes(SimpleConstructorArgs, ComplexConstructorArgs).ToArray();
        }

        /// <summary>
        /// Build constructor arg types and validate that all args are present
        /// </summary>
        static IEnumerable<Type> CompileConstructorArgTypes(
            IEnumerable<(int index, int argIndex, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)> simpleConstructorArgs,
            IEnumerable<(int argIndex, Type constuctorArgType, ObjectPropertyGraph value)> complexConstructorArgs)
        {
            var args = simpleConstructorArgs
                .Select(a => (a.argIndex, a.resultPropertyType))
                .Concat(complexConstructorArgs
                    .Select(a => (a.argIndex, a.constuctorArgType)))
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
        public IEnumerable<IGrouping<object[], object[]>> GroupAndFilterData(IEnumerable<object[]> rows)
        {
            return rows
                .GroupBy(r => 
                    this.PrimaryKeyColumns.Select(i => r[i]).ToArray(), 
                    ArrayComparer<object>.Instance)
                .Where(g => g.Key.All(k => k != null && k != DBNull.Value));
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
        public IEnumerable<object[]> GetDataRowsForSimpleProperty(IEnumerable<object[]> rows, int[] simplePropPrimaryKeyColumns)
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
                .Where(r => simplePropPrimaryKeyColumns.All(x => r[x] != null && r[x] != DBNull.Value))
                .GroupBy(d => GetUniqueIdForSimpleProp(d, simplePropPrimaryKeyColumns))
                .Select(Enumerable.First);
        }

        public override string ToString()
        {
            var primaryKeys = $"PrimaryKeyColumns: [{PrimaryKeyColumns.JoinString(",")}]";

            var simpleProps = SimpleProps
                .Select(p => $"{p.name}: {{ index: {p.index}, rids: [{p.primaryKeyColumns.JoinString(",")}], resultPropertyType: {p.resultPropertyType?.Name ?? "null"}, dataCellType: {p.dataCellType?.Name ?? "null"} }}");

            var complexProps = ComplexProps.Select(p => $"{p.name}:\n  {p.value.ToString().Replace("\n", "\n  ")}");

            var simpleConstructorArgs = SimpleConstructorArgs
                .Select(p => $"CArg_{p.argIndex}: {{ index: {p.index}, rids: [{p.primaryKeyColumns.JoinString(",")}], resultPropertyType: {p.resultPropertyType?.Name ?? "null"}, dataCellType: {p.dataCellType?.Name ?? "null"} }}");

            var complexConstructorArgs = ComplexConstructorArgs.Select(p => $"CArg_{p.argIndex}:\n  {p.value.ToString().Replace("\n", "\n  ")}");

            return new [] { ObjectType.FullName, primaryKeys }
                .Concat(simpleConstructorArgs)
                .Concat(complexConstructorArgs)
                .Concat(simpleProps)
                .Concat(complexProps)
                .JoinString("\n");
        }
    }
}