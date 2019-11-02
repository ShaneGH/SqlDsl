using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// An object which can get the value of an object
    /// </summary>
    public class ValueGetter<TObject> : IValueGetter
    {
        /// <summary>
        /// A getter for data which is not an enumerable data cell type (e.g. int, string)
        /// </summary>
        public readonly Func<IEnumerable<object>, ILogger, TObject> Get;
        
        /// <summary>
        /// A getter for data which is an enumerable data cell type (e.g. BLOB (byte[]))
        /// </summary>
        public readonly Func<IEnumerable<object>, ILogger, TObject> GetEnumerable;

        public ValueGetter(
            Func<IEnumerable<object>, ILogger, TObject> getter,
            Func<IEnumerable<object>, ILogger, TObject> enumerableGetter = null)
        {
            Get = getter;
            GetEnumerable = enumerableGetter ?? CannotGetForEnumerable;
        }

        static TObject CannotGetForEnumerable(IEnumerable<object> y, ILogger z)
        {
            throw new InvalidOperationException("Cannot use this object to get values for a property with an enumerable cell type.");
        }

        object IValueGetter.Get(IEnumerable<DataRowValue> values, ILogger logger) => Get(values.Select(x => x.ToObj()), logger);

        object IValueGetter.Get(IEnumerable<object> values, ILogger logger) => Get(values, logger);

        object IValueGetter.GetEnumerable(IEnumerable<DataRowValue> values, ILogger logger) => GetEnumerable(values.Select(x => x.ToObj()), logger);
    }
    
    /// <summary>
    /// An object which can get the value of an object
    /// </summary>
    public interface IValueGetter
    {
        /// <summary>
        /// A getter for data which is not an enumerable data cell type (e.g. int, string)
        /// </summary>
        object Get(IEnumerable<DataRowValue> values, ILogger logger);
        
        /// <summary>
        /// A getter for data which is not an enumerable data cell type (e.g. int, string)
        /// </summary>
        object Get(IEnumerable<object> values, ILogger logger);
        
        /// <summary>
        /// A getter for data which is an enumerable data cell type (e.g. BLOB (byte[]))
        /// </summary>
        object GetEnumerable(IEnumerable<DataRowValue> values, ILogger logger);
    }
}
