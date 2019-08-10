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
        readonly Constructor<T> _constructor;
        public T ___LastResult => _results[_results.Count - 1];
        List<(int, ITheMoFoBitchParser)> _cArgParsers = new List<(int, ITheMoFoBitchParser)>();
        readonly SonOfTheMoFoBitchParser<T> _objectBuilder;

        public TheMoFoBitchParser(IDataRecord reader, SonOfTheMoFoBitchParser<T> objectBuilder)
        {             
            _keyMonitor = new KeyMonitor(reader, objectBuilder.PrimaryKeyColumns);
            _reader = reader;
            _constructor = new Constructor<T>(objectBuilder.ObjectPropertyGraph);
            _objectBuilder = objectBuilder;

            foreach (var constructorArg in objectBuilder.ComplexCArgParsers)
            {
                // TODO: cache
                var parserType = typeof(TheMoFoBitchParser<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(constructorArg.value.ForType) ?? constructorArg.value.ForType);

                _cArgParsers.Add((
                    constructorArg.cArgIndex,
                    (ITheMoFoBitchParser)Activator.CreateInstance(parserType, new object[] { reader, constructorArg.value })));
            }

            foreach (var constructorArg in objectBuilder.ListCArgParsers)
            {
                _cArgParsers.Add((
                    constructorArg.ArgIndex,
                    TheMiniParser.Build(constructorArg.DataCellType, reader, constructorArg.Index, constructorArg.PrimaryKeyColumns)));
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
                _constructor.ParseRow(_reader);
            
            return result;
        }

        public bool BuildObject()
        {
            // left join did not find anything
            if (!_keyMonitor.AtLeastOneRecordFound)
                return false;

            foreach (var argParser in _cArgParsers)
                _constructor.ReferenceObjects[argParser.Item1] = argParser.Item2.Flush();

            var result = _constructor.Build();
            _results.Add(result);
            _keyMonitor.Reset();
            return true;
        }
    }
}