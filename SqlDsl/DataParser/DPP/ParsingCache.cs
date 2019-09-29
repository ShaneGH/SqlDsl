using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using SqlDsl.Utils;

namespace SqlDsl.DataParser.DPP
{
    public class ParsingCache<T> : IParsingCache
    {
        private readonly KeyMonitor _keyMonitor;
        private readonly IDataRecord _reader;
        protected List<T> Results = new List<T>(4);
        readonly Builder<T> _builder;
        List<(int, IParsingCache)> _complexCArgParsers = new List<(int, IParsingCache)>();
        List<(string, IParsingCache)> _complexPropParsers = new List<(string, IParsingCache)>();

        public ParsingCache(IDataRecord reader, ObjectPropertyGraphWrapper<T> objectBuilder)
        {             
            _keyMonitor = new KeyMonitor(reader, objectBuilder.PrimaryKeyColumns);
            _reader = reader;
            _builder = new Builder<T>(objectBuilder.ObjectPropertyGraph);

            foreach (var (index, parser) in objectBuilder.ComplexCArgParsers)
            {
                // TODO: cache
                var parserType = typeof(ParsingCache<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(parser.ForType) ?? parser.ForType);

                // TODO: reflection
                _complexCArgParsers.Add((
                    index,
                    (IParsingCache)Activator.CreateInstance(parserType, new object[] { reader, parser })));
            }

            foreach (var (name, parser) in objectBuilder.ComplexPropParsers)
            {
                // TODO: cache
                var parserType = typeof(ParsingCache<>)
                    .MakeGenericType(ReflectionUtils.GetIEnumerableType(parser.ForType) ?? parser.ForType);

                // TODO: reflection
                _complexPropParsers.Add((
                    name,
                    (IParsingCache)Activator.CreateInstance(parserType, new object[] { reader, parser })));
            }

            foreach (var constructorArg in objectBuilder.ListCArgParsers)
            {
                _complexCArgParsers.Add((
                    constructorArg.ArgIndex,
                    MultiRowCellParser.Build(constructorArg.DataCellType, reader, constructorArg.Index, constructorArg.PrimaryKeyColumns)));
            }

            foreach (var constructorArg in objectBuilder.ListPropParsers)
            {
                _complexPropParsers.Add((
                    constructorArg.Name,
                    MultiRowCellParser.Build(constructorArg.DataCellType, reader, constructorArg.Index, constructorArg.PrimaryKeyColumns)));
            }
        }

        public IEnumerable Flush()
        {
            BuildObjectAndAddToResults();
            
            var results = Results;
            Results = new List<T>(4);
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
                BuildObjectAndAddToResults();
                result = true;

                // recalculate first and changed after
                // build object
                first = !_keyMonitor.AtLeastOneRecordFound;
                changed = _keyMonitor.RecordHasChanged();
            }
            
            foreach (var (_, parser) in _complexCArgParsers)
                parser.OnNextRow();
            
            foreach (var (_, parser) in _complexPropParsers)
                parser.OnNextRow();

            // record is first in this set
            if (first && changed)
                _builder.ParseRow(_reader);
            
            return result;
        }

        protected bool BuildObjectAndAddToResults()
        {
            // left join did not find anything
            if (!_keyMonitor.AtLeastOneRecordFound)
                return false;

            foreach (var (index, parser) in _complexCArgParsers)
                _builder.ReferenceObjectCArgs[index] = parser.Flush();

            foreach (var (name, parser) in _complexPropParsers)
                _builder.ReferenceObjectProps.Add(new KeyValuePair<string, object>(name, parser.Flush()));

            var result = _builder.Build();
            Results.Add(result);
            _keyMonitor.Reset();
            return true;
        }
    }
}