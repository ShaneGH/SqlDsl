using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using SqlDsl.Utils;

namespace SqlDsl.DataParser.DPP
{
    /// <summary>
    /// Wraps an ObjectPropertyGraph providing extra properties for building objects
    /// </summary>
    public class ObjectPropertyGraphWrapper<T> : IObjectPropertyGraphWrapper
    {
        //TODO: inherit from ObjectPropertyGraph?

        static readonly Type _forType = typeof(T);
        public Type ForType => _forType;
        public readonly ObjectPropertyGraph ObjectPropertyGraph;
        public readonly ReadOnlyCollection<(int cArgIndex, IObjectPropertyGraphWrapper value)> ComplexCArgParsers;
        public readonly ReadOnlyCollection<(string name, IObjectPropertyGraphWrapper value)> ComplexPropParsers;
        public readonly ReadOnlyCollection<SimpleConstructorArg> ListCArgParsers;
        public readonly ReadOnlyCollection<SimpleProp> ListPropParsers;
        public readonly ReadOnlyCollection<SimpleConstructorArg> SimpleConstructorArgs;
        public readonly ConstructorInfo Constructor;
        public readonly int[] PrimaryKeyColumns;
        public readonly int ConstructorArgLength;
        
        public ObjectPropertyGraphWrapper(ObjectPropertyGraph objectPropertyGraph)
        {             
            ObjectPropertyGraph = objectPropertyGraph;
            Constructor = GetConstructor(objectPropertyGraph);
            SimpleConstructorArgs = objectPropertyGraph.SimpleConstructorArgs.Where(x => x.PrimaryKeyColumns.Length == 0).ToList().AsReadOnly();
            ListCArgParsers = objectPropertyGraph.SimpleConstructorArgs.Where(x => x.PrimaryKeyColumns.Length > 0).ToList().AsReadOnly();
            ListPropParsers = objectPropertyGraph.SimpleProps.Where(x => x.PrimaryKeyColumns.Length > 0).ToList().AsReadOnly();
            PrimaryKeyColumns = objectPropertyGraph.PrimaryKeyColumns;
            ConstructorArgLength = objectPropertyGraph.SimpleConstructorArgs.Count() + objectPropertyGraph.ComplexConstructorArgs.Count();

            var complexCargParsers = new List<(int, IObjectPropertyGraphWrapper)>();
            foreach (var constructorArg in objectPropertyGraph.ComplexConstructorArgs)
            {
                var parserType = typeof(ObjectPropertyGraphWrapper<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(constructorArg.Value.ObjectType) ?? constructorArg.Value.ObjectType);

                // TODO: reflection
                complexCargParsers.Add((
                    constructorArg.ArgIndex,
                    (IObjectPropertyGraphWrapper)Activator.CreateInstance(parserType, new object[] { constructorArg.Value })));
            }

            ComplexCArgParsers = complexCargParsers.AsReadOnly();

            var complexPropParsers = new List<(string, IObjectPropertyGraphWrapper)>();
            foreach (var prop in objectPropertyGraph.ComplexProps)
            {
                var parserType = typeof(ObjectPropertyGraphWrapper<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(prop.Value.ObjectType) ?? prop.Value.ObjectType);

                // TODO: reflection
                complexPropParsers.Add((
                    prop.Name,
                    (IObjectPropertyGraphWrapper)Activator.CreateInstance(parserType, new object[] { prop.Value })));
            }

            ComplexPropParsers = complexPropParsers.AsReadOnly();
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

        // public T BuildObject(IEnumerable[] constructorArgs, IEnumerable<(int, ITheMoFoBitchParser)> cArgParsers)
        // {
        //     foreach (var complexCArg in cArgParsers)
        //         constructorArgs[complexCArg.Item1] = complexCArg.Item2.Flush();

        //     var cArgsNonEnumerable = constructorArgs
        //         .Select((x, i) =>
        //         {
        //             var arg = Constructor.GetParameters()[i];
        //             var (isEnum, tBuilder) = Enumerables.CreateCollectionExpression(arg.ParameterType, Expression.Constant(x));
        //             if (!isEnum)
        //                 return x.Cast<object>().SingleOrDefault();   // TODO: left join on 1 -> 1

        //             return Expression
        //                 .Lambda(tBuilder, Enumerable.Empty<ParameterExpression>())
        //                 .Compile()
        //                 .DynamicInvoke();
        //         })
        //         .Select(result => DBNull.Value.Equals(result)
        //             ? null
        //             : result);

        //     return (T)Constructor.Invoke(cArgsNonEnumerable.ToArray());
        // }


        // public void SetSimpleConstructorArgs(IEnumerable[] values, IDataRecord reader)
        // {
        //     foreach (var simple in SimpleConstructorArgs)
        //     {
        //         // TODO: can I reuse these arrays?
        //         var arr = Array.CreateInstance(simple.DataCellType, 1);
        //         arr.SetValue(reader.GetValue(simple.Index), 0);
        //         values[simple.ArgIndex] = arr;
        //     }
        // }
    }
}