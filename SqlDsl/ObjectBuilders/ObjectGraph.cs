using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A generic object graph which can be converted into a concrete class
    /// </summary>
    public class ObjectGraph
    {   
        /// <summary>
        /// Simple properties such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleProps { get; set; }
        
        /// <summary>
        /// Complex properties will have properties of their own
        /// </summary>
        public IEnumerable<(string name, IEnumerable<ObjectGraph> value)> ComplexProps { get; set; }
        
        /// <summary>
        /// Simple constructor args such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleConstructorArgs { get; set; }
        
        /// <summary>
        /// Complex constructor args will have properties of their own
        /// </summary>
        public IEnumerable<(int argIndex, IEnumerable<ObjectGraph> value)> ComplexConstructorArgs { get; set; }

        /// <summary>
        /// The type of the constructor args to be used with this object
        /// </summary>
        public Type[] ConstructorArgTypes { get; set; }

        public override string ToString()
        {
            var simple = SimpleProps
                .OrEmpty()
                .Select(ps => $"S_{ps.name}:\n  [{ps.value.Select(p => $"\n    {p}").JoinString("")}\n  ]")
                .JoinString("\n");

            var complex = ComplexProps
                .OrEmpty()
                .Select(ps => 
                {
                    var propStrings = ps.value
                        .Select(p => $"{p.ToString().Replace("\n", "\n    ")}")
                        .JoinString("");

                    return $"C_{ps.name}:\n  {{\n    {propStrings}\n  }}";
                })
                .JoinString("\n");

            return $"{simple}\n{complex}";
        }
    }
}
