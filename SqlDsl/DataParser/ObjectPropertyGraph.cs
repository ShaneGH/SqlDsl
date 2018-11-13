using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    //TODO: this class needs a cleanup

    /// <summary>
    /// Represents an object with it's property names
    /// </summary>
    public class ObjectPropertyGraph
    {
        //TODO: change IEnumerables to arrays and optimize code usage

        /// <summary>
        /// Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// Type can be null if the property is generated by sql, (e.g. if property is ##rowid)
        /// </summary>
        public readonly IEnumerable<(int index, string name, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType)> SimpleProps;
        
        /// <summary>
        /// Properties of an object which have sub properies
        /// </summary>
        public readonly IEnumerable<(string name, ObjectPropertyGraph value)> ComplexProps;

        /// <summary>
        /// Constructor args of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly IEnumerable<(int index, int argIndex, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType)> SimpleConstructorArgs;
        
        /// <summary>
        /// Constructor args of an object which have sub properies
        /// </summary>
        public readonly IEnumerable<(int argIndex, Type constuctorArgType, ObjectPropertyGraph value)> ComplexConstructorArgs;

        /// <summary>
        /// Constructor args of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly Type[] ConstructorArgTypes;
        
        /// <summary>
        /// A composite of the row numbers which point to this object
        /// </summary>
        public readonly IEnumerable<int> RowIdColumnNumbers;
        // if RowIdColumnNumbers are invalid
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
        /// <param name="rowIdColumnNumbers">A composite of the row numbers which point to this object</param>
        public ObjectPropertyGraph(
            Type objectType,
            IEnumerable<(int index, string name, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType)> simpleProps, 
            IEnumerable<(string name, ObjectPropertyGraph value)> complexProps, 
            IEnumerable<int> rowIdColumnNumbers,
            IEnumerable<(int index, int argIndex, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType)> simpleConstructorArgs = null,
            IEnumerable<(int argIndex, Type constuctorArgType, ObjectPropertyGraph value)> complexConstructorArgs = null)
        {
            ObjectType = objectType ?? throw new ArgumentNullException(nameof(objectType));
            SimpleProps = simpleProps.OrEmpty();
            ComplexProps = complexProps.OrEmpty();
            RowIdColumnNumbers = rowIdColumnNumbers.OrEmpty();
            SimpleConstructorArgs = simpleConstructorArgs.OrEmpty();
            ComplexConstructorArgs = complexConstructorArgs.OrEmpty();
            ConstructorArgTypes = CompileConstructorArgTypes(SimpleConstructorArgs, ComplexConstructorArgs).ToArray();
        }

        /// <summary>
        /// Build constructor arg types and validate that all args are present
        /// </summary>
        static IEnumerable<Type> CompileConstructorArgTypes(
            IEnumerable<(int index, int argIndex, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType)> simpleConstructorArgs,
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
        /// Get the unique id of a row, in the context of a simple prop of this object
        /// </summary>
        public string GetUniqueIdForSimpleProp(object[] row, IEnumerable<int> simplePropRowNumberColumnIds)
        {
            // TODO: this method is used a lot.
            // Can results be cached or more efficient method used?
            return RowIdColumnNumbers
                .Concat(simplePropRowNumberColumnIds.OrEmpty())   
                .Select(r => row[r].ToString())
                .JoinString(";");
        }

        public override string ToString()
        {
            var rowNumbers = $"RowIdColumnNumbers: [{RowIdColumnNumbers.JoinString(",")}]";

            var simpleProps = SimpleProps
                .Select(p => $"{p.name}: {{ index: {p.index}, rids: [{p.rowNumberColumnIds.JoinString(",")}], resultPropertyType: {p.resultPropertyType?.Name ?? "null"}, dataCellType: {p.dataCellType?.Name ?? "null"} }}");

            var complexProps = ComplexProps.Select(p => $"{p.name}:\n  {p.value.ToString().Replace("\n", "\n  ")}");

            var simpleConstructorArgs = SimpleConstructorArgs
                .Select(p => $"CArg_{p.argIndex}: {{ index: {p.index}, rids: [{p.rowNumberColumnIds.JoinString(",")}], resultPropertyType: {p.resultPropertyType?.Name ?? "null"}, dataCellType: {p.dataCellType?.Name ?? "null"} }}");

            var complexConstructorArgs = ComplexConstructorArgs.Select(p => $"CArg_{p.argIndex}:\n  {p.value.ToString().Replace("\n", "\n  ")}");

            return new [] { ObjectType.FullName, rowNumbers }
                .Concat(simpleConstructorArgs)
                .Concat(complexConstructorArgs)
                .Concat(simpleProps)
                .Concat(complexProps)
                .JoinString("\n");
        }
    }
}