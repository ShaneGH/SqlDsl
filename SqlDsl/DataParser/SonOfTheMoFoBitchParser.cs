using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.ObjectBuilders;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    public class SonOfTheMoFoBitchParser<T> : ISonOfTheTheMoFoBitchParser
    {
        static readonly Type _forType = typeof(T);
        public Type ForType => _forType;
        public readonly ReadOnlyCollection<(int cArgIndex, ISonOfTheTheMoFoBitchParser value)> ComplexCArgParsers;
        public readonly ReadOnlyCollection<(int colIndex, int cArgIndex, int[] primaryKeyColumns, Type forType)> ListCArgParsers;
        public readonly ConstructorInfo Constructor;
        public readonly SimpleConstructorArg[] SimpleConstructorArgs;
        public readonly int[] PrimaryKeyColumns;

        // TODO: delete this prop
        public readonly int ConstructorArgLength;
        
        public SonOfTheMoFoBitchParser(ObjectPropertyGraph objectPropertyGraph)
        {             
            Constructor = GetConstructor(objectPropertyGraph);
            SimpleConstructorArgs = objectPropertyGraph.SimpleConstructorArgs.Where(x => x.PrimaryKeyColumns.Length == 0).ToArray();
            PrimaryKeyColumns = objectPropertyGraph.PrimaryKeyColumns;
            ConstructorArgLength = objectPropertyGraph.SimpleConstructorArgs.Count() + objectPropertyGraph.ComplexConstructorArgs.Count();

            var complexCargParsers = new List<(int, ISonOfTheTheMoFoBitchParser)>();
            var listCargParsers = new List<(int index, int, int[] primaryKeyColumns, Type)>();
            foreach (var constructorArg in objectPropertyGraph.ComplexConstructorArgs)
            {
                var parserType = typeof(SonOfTheMoFoBitchParser<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(constructorArg.Value.ObjectType) ?? constructorArg.Value.ObjectType);

                complexCargParsers.Add((
                    constructorArg.ArgIndex,
                    (ISonOfTheTheMoFoBitchParser)Activator.CreateInstance(parserType, new object[] { constructorArg.Value })));
            }

            foreach (var constructorArg in objectPropertyGraph.SimpleConstructorArgs.Where(x => x.PrimaryKeyColumns.Length > 0))
            {
                listCargParsers.Add((constructorArg.Index, constructorArg.ArgIndex, constructorArg.PrimaryKeyColumns, constructorArg.DataCellType));
            }

            ComplexCArgParsers = complexCargParsers.AsReadOnly();
            ListCArgParsers = listCargParsers.AsReadOnly();
        }
        
        static ConstructorInfo GetConstructor(ObjectPropertyGraph objectPropertyGraph)
        {
            var args = new Type[objectPropertyGraph.SimpleConstructorArgs.Count() + objectPropertyGraph.ComplexConstructorArgs.Count()];
            foreach (var x in objectPropertyGraph.ComplexConstructorArgs)
            {
                args[x.ArgIndex] = x.ConstuctorArgType;
            }
            
            foreach (var x in objectPropertyGraph.SimpleConstructorArgs)
            {
                args[x.ArgIndex] = objectPropertyGraph.ConstructorArgTypes[x.ArgIndex];
            }

            return typeof(T).GetConstructor(args);
        }

        public T BuildObject(IEnumerable[] constructorArgs, IEnumerable<(int, ITheMoFoBitchParser)> cArgParsers)
        {
            foreach (var complexCArg in cArgParsers)
                constructorArgs[complexCArg.Item1] = complexCArg.Item2.Flush();

            var cArgsNonEnumerable = constructorArgs
                .Select((x, i) =>
                {
                    var arg = Constructor.GetParameters()[i];
                    var (isEnum, tBuilder) = Enumerables.CreateCollectionExpression(arg.ParameterType, Expression.Constant(x));
                    if (!isEnum)
                        return x.Cast<object>().SingleOrDefault();   // TODO: left join on 1 -> 1

                    return Expression
                        .Lambda(tBuilder, Enumerable.Empty<ParameterExpression>())
                        .Compile()
                        .DynamicInvoke();
                })
                .Select(result => DBNull.Value.Equals(result)
                    ? null
                    : result);

            return (T)Constructor.Invoke(cArgsNonEnumerable.ToArray());
        }


        public void SetSimpleConstructorArgs(IEnumerable[] values, IDataRecord reader)
        {
            foreach (var simple in SimpleConstructorArgs)
            {
                // TODO: can I reuse these arrays?
                var arr = Array.CreateInstance(simple.DataCellType, 1);
                arr.SetValue(reader.GetValue(simple.Index), 0);
                values[simple.ArgIndex] = arr;
            }
        }
    }
}