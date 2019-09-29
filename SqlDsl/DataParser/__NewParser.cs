using System.Data.Common;
using SqlDsl.DataParser.DPP;
using SqlDsl.Mapper;

namespace SqlDsl.DataParser
{
    public interface IParser<T>
    {
        (bool, T) Next();
    }

    class PropMapValueParser<T> : __NewParser<PropMapValue<T>>, IParser<T>
    {
        public PropMapValueParser(DbDataReader reader, ObjectPropertyGraph objectPropertyGraph)
            : base (reader, objectPropertyGraph)
        {
        }

        (bool, T) IParser<T>.Next()
        {
            var (success, result) = base.Next();
            if (!success)
                return (false, default(T));

            var val = result.Value;
            result.Dispose();

            return (true, val);
        }
    }

    public class __NewParser<T> : ParsingCache<T>, IParser<T>
    {
        private readonly DbDataReader _reader;
        bool _hasData;

        public __NewParser(DbDataReader reader, ObjectPropertyGraph objectPropertyGraph)
            : base (reader, new ObjectPropertyGraphWrapper<T>(objectPropertyGraph))
        {             
            _reader = reader;
            _hasData = _reader.Read();
        }

        public (bool, T) Next()
        {
            if (!_hasData)
                return (false, default(T));

            do
            {
                if (OnNextRow())
                    return (true, Results[Results.Count - 1]);

            } while (_hasData = _reader.Read());

            return BuildObjectAndAddToResults()
                ? (true, Results[Results.Count - 1])
                : (false, default(T));
        }
    }
}