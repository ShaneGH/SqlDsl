using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{

    /// <summary>
    /// A generic builder interface
    /// </summary>
    public interface IBuilder
    {
        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        object Build(ObjectGraph values, ILogger logger);
    }

    /// <summary>
    /// A compiled (at runtime) builder class which builds a concrete object from an object graph
    /// </summary>
    public class Builder<T> : IBuilder
    {
        /// <summary>
        /// Compiled function to build an object
        /// </summary>
        readonly Func<ObjectGraph, ILogger, T> BuildObject;
        
        /// <summary>
        /// Compiled function to set all null enumerable properties of an object to empty
        /// </summary>
        readonly Action<T, IEnumerable<string>> AddEmptyEnumerables;

        public Builder()
        {
            BuildObject = Objects.CompileObjectBuilder<T>();
            AddEmptyEnumerables = Enumerables.CompileEnumerabeAdder<T>();
        }

        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        public T Build(ObjectGraph values, ILogger logger) 
        {
            var obj = BuildObject(values, logger);
            var enumProps = (values?.SimpleProps)
                .OrEmpty()
                .Where(isEnumerableDataCell)
                .Select(name);

            AddEmptyEnumerables(obj, enumProps);

            return obj;

            bool isEnumerableDataCell((string name, IEnumerable<object> value, Action<object, IEnumerable<object>, ILogger> customSetter, bool isEnumerableDataCell) c) => c.isEnumerableDataCell;
            string name((string name, IEnumerable<object> value, Action<object, IEnumerable<object>, ILogger> customSetter, bool isEnumerableDataCell) c) => c.name;
        }

        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        object IBuilder.Build(ObjectGraph values, ILogger logger) => Build(values, logger);
    }
}
