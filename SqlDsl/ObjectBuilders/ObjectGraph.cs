using SqlDsl.Utils;
using System.Collections.Generic;
using System.Linq;

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
