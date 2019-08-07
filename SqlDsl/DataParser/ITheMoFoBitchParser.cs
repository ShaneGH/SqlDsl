using System;
using System.Collections;

namespace SqlDsl.DataParser
{
    public interface ITheMoFoBitchParser
    {
        bool OnNextRow();
        IEnumerable Flush();
    }
    
    public interface ISonOfTheTheMoFoBitchParser
    {
        /// <summary>
        /// The generic argument of the parser. This is not necessartly the type 
        /// of object which will will be parsed out
        /// </summary>
        Type ForType { get; }
    }
}