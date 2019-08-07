using System.Data.Common;

namespace SqlDsl.DataParser
{
    public class TheMoFoParser<T> : TheMoFoBitchParser<T>
    {
        private readonly DbDataReader _reader;
        bool _hasData;

        public TheMoFoParser(DbDataReader reader, ObjectPropertyGraph objectPropertyGraph)
            : base (reader, new SonOfTheMoFoBitchParser<T>(objectPropertyGraph))
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
                    return (true, (T)___LastResult);

            } while (_hasData = _reader.Read());

            return BuildObject()
                ? (true, (T)___LastResult)
                : (false, default(T));
        }
    }
}