using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Mapper;
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
        object Build(ObjectGraph values, IPropMapValueCache propMapValueCache, ILogger logger);
    }
    
    /// <summary>
    /// A generic builder interface
    /// </summary>
    public interface IBuilder<T>
    {
        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        T Build(ObjectGraph values, IPropMapValueCache propMapValueCache, ILogger logger);
    }

    /// <summary>
    /// A compiled (at runtime) builder class which builds a concrete object from an object graph
    /// </summary>
    public class Builder<T> : IBuilder<T>, IBuilder
    {
        /// <summary>
        /// Compiled function to build an object
        /// </summary>
        readonly Func<ObjectGraph, IPropMapValueCache, ILogger, T> BuildObject;
        
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
        public T Build(ObjectGraph values, IPropMapValueCache propMapValueCache, ILogger logger) 
        {
            var obj = BuildObject(values, propMapValueCache, logger);
            var enumProps = (values?.GetSimpleProps())
                .OrEmpty()
                .Where(isEnumerableDataCell)
                .Select(name);

            AddEmptyEnumerables(obj, enumProps);

            return obj;

            bool isEnumerableDataCell((string, IEnumerable<object>, bool isEnumerableDataCell) c) => c.isEnumerableDataCell;
            string name((string name, IEnumerable<object>, bool) c) => c.name;
        }

        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        object IBuilder.Build(ObjectGraph values, IPropMapValueCache propMapValueCache, ILogger logger) => Build(values, propMapValueCache, logger);
    }
}
