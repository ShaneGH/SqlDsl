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
                .Select(ps => $"{ps.name}: [{ps.value.Select(p => $"\n\t{p}").JoinString("")}\n\t]")
                .JoinString("\n");

            var complex = ComplexProps
                .OrEmpty()
                .Select(ps => 
                {
                    var propStrings = ps.value
                        .Select(p => $"\n\t{p.ToString().Replace("\n", "\n\t")}");

                    return $"{ps.name}: {{{propStrings.JoinString("")}\n\t}}";
                })
                .JoinString("\n");

            return $"{simple}\n{complex}";
        }
    }
}
