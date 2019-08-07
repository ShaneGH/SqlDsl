using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    public class TheMoFoBitchParser<T> : ITheMoFoBitchParser
    {
        private readonly KeyMonitor _keyMonitor;
        private readonly IDataRecord _reader;
        private List<T> _results = new List<T>(4);
        readonly IEnumerable[] _constructorArgs;
        public T ___LastResult => _results[_results.Count - 1];
        List<(int, ITheMoFoBitchParser)> _cArgParsers = new List<(int, ITheMoFoBitchParser)>();
        readonly SonOfTheMoFoBitchParser<T> _objectBuilder;

        public TheMoFoBitchParser(IDataRecord reader, SonOfTheMoFoBitchParser<T> objectBuilder)
        {             
            _keyMonitor = new KeyMonitor(reader, objectBuilder.PrimaryKeyColumns);
            _reader = reader;
            _constructorArgs = new IEnumerable<object>[objectBuilder.ConstructorArgLength];
            _objectBuilder = objectBuilder;

            foreach (var constructorArg in objectBuilder.ComplexCArgParsers)
            {
                var parserType = typeof(TheMoFoBitchParser<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(constructorArg.value.ForType) ?? constructorArg.value.ForType);

                _cArgParsers.Add((
                    constructorArg.cArgIndex,
                    (ITheMoFoBitchParser)Activator.CreateInstance(parserType, new object[] { reader, constructorArg.value })));
            }

            foreach (var constructorArg in objectBuilder.ListCArgParsers)
            {
                var parserType = typeof(TheMiniParser<>)
                    .MakeGenericType(constructorArg.forType);

                _cArgParsers.Add((
                    constructorArg.cArgIndex,
                    (ITheMoFoBitchParser)Activator.CreateInstance(parserType, new object[] { reader, constructorArg.colIndex, constructorArg.primaryKeyColumns })));
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
                _objectBuilder.SetSimpleConstructorArgs(_constructorArgs, _reader);
            
            return result;
        }

        public bool BuildObject()
        {
            // left join did not find anything
            if (!_keyMonitor.AtLeastOneRecordFound)
                return false;

            var result = _objectBuilder.BuildObject(_constructorArgs, _cArgParsers);
            _results.Add(result);
            _keyMonitor.Reset();
            return true;
        }
    }
}