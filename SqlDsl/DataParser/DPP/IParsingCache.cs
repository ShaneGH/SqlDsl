using System.Collections;

namespace SqlDsl.DataParser.DPP
{
    public interface IParsingCache
    {
        bool OnNextRow();
        IEnumerable Flush();
    }
}