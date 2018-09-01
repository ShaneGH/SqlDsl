using SqlDsl.Utils;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Represents an object with it's property names
    /// </summary>
    class ObjectPropertyGraph
    {
        /// <summary>
        /// Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly IEnumerable<(int index, string name)> SimpleProps;
        
        /// <summary>
        /// Properties of an object which have sub properies
        /// </summary>
        public readonly IEnumerable<(string name, ObjectPropertyGraph value)> ComplexProps;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query.
        /// </param>
        public ObjectPropertyGraph(IEnumerable<string> colNames)
            : this(colNames.Select((n, i) => (i, n.Split('.').ToArray())))
        {
        }

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query. Names are "." delimited to split a string into parts.
        /// </param>
        ObjectPropertyGraph(IEnumerable<(int index, string[] name)> colNames)
        {
            var simpleProps = new HashSet<(int index, string propertyName)>();
            var complexProps = new List<(int index, string propertyName, IEnumerable<string> childProps)>();

            foreach (var col in colNames)
            {
                // if there is only one name, the property belongs to this object
                if (col.name.Length == 1)
                {
                    simpleProps.Add((col.index, col.name[0]));
                }
                // if there are more than one, the property belongs to a child of this object
                else if (col.name.Length > 1)
                {
                    // separate the property from this object (index == 0) from the properties of
                    // child objects
                    complexProps.Add((col.index, col.name[0], col.name.Skip(1)));
                }
            }

            var cProps = complexProps
                // group properties by child property name
                .GroupBy(x => x.propertyName)
                // build child property graph
                .Select(group => (group.Key, new ObjectPropertyGraph(group.Select(x => (x.index, x.childProps.ToArray())))));
            
            SimpleProps = simpleProps;
            ComplexProps = cProps.Enumerate();
        }

        public override string ToString() =>
            SimpleProps
                .Select(p => $"{p.name}: {p.index}")
                .Concat(ComplexProps.Select(p => $"{p.name}:\n  {p.value.ToString().Replace("\n", "\n  ")}"))
                .JoinString("\n");
    }
}
