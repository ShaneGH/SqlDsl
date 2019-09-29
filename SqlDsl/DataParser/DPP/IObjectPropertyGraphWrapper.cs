using System;

namespace SqlDsl.DataParser.DPP
{    
    public interface IObjectPropertyGraphWrapper
    {
        /// <summary>
        /// The generic argument of the parser. This is not necessartly the type 
        /// of object which will will be parsed out
        /// </summary>
        Type ForType { get; }
    }
}