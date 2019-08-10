using SqlDsl.DataParser;
using System;
using System.Collections.Generic;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// An object which can set the value of a object property or field
    /// </summary>
    public class PropertySetter<TObject>
    {
        /// <summary>
        /// A setter for data which is not an enumerable data cell type (e.g. int, string)
        /// </summary>
        public readonly Action<TObject, IEnumerable<object>, ILogger> Set;
        
        /// <summary>
        /// A setter for data which is an enumerable data cell type (e.g. BLOB (byte[]))
        /// </summary>
        public readonly Action<TObject, IEnumerable<object>, ILogger> SetEnumerable;

        public PropertySetter(
            Action<TObject, IEnumerable<object>, ILogger> setter,
            Action<TObject, IEnumerable<object>, ILogger> enumerableSetter = null)
        {
            Set = setter;
            SetEnumerable = enumerableSetter ?? CannotSetForEnumerable;
        }

        static void CannotSetForEnumerable(TObject x, IEnumerable<object> y, ILogger z)
        {
            throw new ParsingException("Cannot use this object to set values for a property with an enumerable cell type.");
        }
    }
}
