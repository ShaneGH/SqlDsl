using System;

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
        object Build(ObjectGraph values);
    }

    /// <summary>
    /// A compiled (at runtime) builder class which builds a concrete object from an object graph
    /// </summary>
    public class Builder<T> : IBuilder
        where T: new()
    {
        /// <summary>
        /// Compiled function to build an object
        /// </summary>
        readonly Func<ObjectGraph, T> BuildObject;
        
        /// <summary>
        /// Compiled function to set all null enumerable properties of an object to empty
        /// </summary>
        readonly Action<T> AddEmptyEnumerables;

        public Builder()
        {
            BuildObject = Objects.CompileObjectBuilder<T>();
            AddEmptyEnumerables = Enumerables.CompileEnumerabeAdder<T>();
        }

        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        public T Build(ObjectGraph values) 
        {
            var obj = BuildObject(values);
            AddEmptyEnumerables(obj);

            return obj;
        }

        /// <summary>
        /// Build a concrete object from an object graph
        /// </summary>
        object IBuilder.Build(ObjectGraph values) => Build(values);
    }
}
