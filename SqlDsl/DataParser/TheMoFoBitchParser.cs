using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.ObjectBuilders;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    public class TheMoFoBitchParser<T> : ITheMoFoBitchParser
    {
        private readonly KeyMonitor _keyMonitor;
        private readonly IDataRecord _reader;
        private List<T> _results = new List<T>(4);
        readonly IEnumerable[] _constructorArgs;
        List<(int, ITheMoFoBitchParser)> _cArgParsers = new List<(int, ITheMoFoBitchParser)>();
        readonly ConstructorInfo _constructor;
        readonly (int index, int argIndex, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)[] _simpleConstructorArgs;
        public T ___LastResult => _results[_results.Count - 1];

        public TheMoFoBitchParser(IDataRecord reader, ObjectPropertyGraph objectPropertyGraph)
        {             
            _keyMonitor = new KeyMonitor(reader, objectPropertyGraph.PrimaryKeyColumns);
            _reader = reader;
            _constructor = GetConstructor(objectPropertyGraph);
            _constructorArgs = new IEnumerable<object>[objectPropertyGraph.SimpleConstructorArgs.Count() + objectPropertyGraph.ComplexConstructorArgs.Count()];
            _simpleConstructorArgs = objectPropertyGraph.SimpleConstructorArgs.Where(x => x.primaryKeyColumns.Length == 0).ToArray();

            foreach (var constructorArg in objectPropertyGraph.ComplexConstructorArgs)
            {
                var parserType = typeof(TheMoFoBitchParser<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(constructorArg.value.ObjectType) ?? constructorArg.value.ObjectType);

                _cArgParsers.Add((
                    constructorArg.argIndex,
                    (ITheMoFoBitchParser)Activator.CreateInstance(parserType, new object[] { reader, constructorArg.value })));
            }

            foreach (var constructorArg in objectPropertyGraph.SimpleConstructorArgs.Where(x => x.primaryKeyColumns.Length > 0))
            {
                var parserType = typeof(TheMiniParser<>)
                    .MakeGenericType(constructorArg.dataCellType);

                _cArgParsers.Add((
                    constructorArg.argIndex,
                    (ITheMoFoBitchParser)Activator.CreateInstance(parserType, new object[] { reader, constructorArg.index, constructorArg.primaryKeyColumns })));
            }
        }

        public IEnumerable Flush()
        {
            BuildObject();
            
            var results = _results;
            _results = new List<T>(4);
            _keyMonitor.Reset();

            return results;
        }

        public bool OnNextRow()
        {
            var result = false;
            var first = !_keyMonitor.AtLeastOneRecordFound;
            var changed = _keyMonitor.RecordHasChanged();

            // record is first in the next set
            if (!first && changed)
            {
                // build object from previous set
                BuildObject();
                result = true;

                // recalculate first and changed after
                // build object
                first = !_keyMonitor.AtLeastOneRecordFound;
                changed = _keyMonitor.RecordHasChanged();
            }
            
            foreach (var parser in _cArgParsers)
                parser.Item2.OnNextRow();

            // record is first in this set
            if (first && changed)
                BuildSimpleConstructorArgs();
            
            return result;
        }

        public void BuildSimpleConstructorArgs()
        {
            foreach (var simple in _simpleConstructorArgs)
            {
                // TODO: can I reuse these arrays?
                var arr = Array.CreateInstance(simple.dataCellType, 1);
                arr.SetValue(_reader.GetValue(simple.index), 0);
                _constructorArgs[simple.argIndex] = arr;
            }
        }

        public bool BuildObject()
        {
            // left join did not find anything
            if (!_keyMonitor.AtLeastOneRecordFound)
                return false;

            foreach (var complexCArg in _cArgParsers)
                _constructorArgs[complexCArg.Item1] = complexCArg.Item2.Flush();

            var cArgsNonEnumerable = _constructorArgs
                .Select((x, i) =>
                {
                    var arg = _constructor.GetParameters()[i];
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

            _results.Add((T)_constructor.Invoke(cArgsNonEnumerable.ToArray()));
            _keyMonitor.Reset();
            return true;
        }
        
        static ConstructorInfo GetConstructor(ObjectPropertyGraph objectPropertyGraph)
        {
            var args = new Type[objectPropertyGraph.SimpleConstructorArgs.Count() + objectPropertyGraph.ComplexConstructorArgs.Count()];
            foreach (var x in objectPropertyGraph.ComplexConstructorArgs)
            {
                args[x.argIndex] = x.constuctorArgType;
            }
            
            foreach (var x in objectPropertyGraph.SimpleConstructorArgs)
            {
                args[x.argIndex] = objectPropertyGraph.ConstructorArgTypes[x.argIndex];
            }

            return typeof(T).GetConstructor(args);
        }
    }
}