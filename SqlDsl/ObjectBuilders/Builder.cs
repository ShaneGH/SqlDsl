using System;
using System.Collections.Generic;

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
        /// <param name="enumerableDbFields">A list of props on the object which are enumerable, and will get data from a single cell. e.g. BLOB => byte[]</param>
        object Build(ObjectGraph values, IEnumerable<string> enumerableDbFields);
    }

    /// <summary>
    /// A compiled (at runtime) builder class which builds a concrete object from an object graph
    /// </summary>
    public class Builder<T> : IBuilder
    {
        /// <summary>
        /// Compiled function to build an object
        /// </summary>
        readonly Func<ObjectGraph, T> BuildObject;
        
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
        /// <param name="enumerableDbFields">A list of props on the object which are enumerable, and will get data from a single cell. e.g. BLOB => byte[]</param>
        public T Build(ObjectGraph values, IEnumerable<string> enumerableDbFields) 
        {
            var obj = BuildObject(values);
            AddEmptyEnumerables(obj, enumerableDbFields);

            return obj;
        }

        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        /// <param name="enumerableDbFields">A list of props on the object which are enumerable, and will get data from a single cell. e.g. BLOB => byte[]</param>
        object IBuilder.Build(ObjectGraph values, IEnumerable<string> enumerableDbFields) => Build(values, enumerableDbFields);
    }
}
