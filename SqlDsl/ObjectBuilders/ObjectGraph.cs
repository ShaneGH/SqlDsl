using System.Collections.Generic;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A generic object graph which can be converted into a concrete class
    /// </summary>
    public class ObjectGraph
    {   
        // TODO: do these nee to be enumerable?
        /// <summary>
        /// Simple properties such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public IEnumerable<(string name, IEnumerable<object> value)> SimpleProps { get; set; }
        
        /// <summary>
        /// Complex properties will have properties of their own
        /// </summary>
        public IEnumerable<(string name, IEnumerable<ObjectGraph> value)> ComplexProps { get; set; }
    }
}
