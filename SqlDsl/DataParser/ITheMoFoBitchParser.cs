using System.Collections;

namespace SqlDsl.DataParser
{
    public interface ITheMoFoBitchParser
    {
        bool OnNextRow();
        IEnumerable Flush();
    }
}