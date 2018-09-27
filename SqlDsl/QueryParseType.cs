using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SqlDsl
{
    /// <summary>
    /// A reference to how results should be parsed
    /// </summary>
    public enum QueryParseType
    {
        /// <summary>
        /// Behave like an ORM. Many to many results may be duplicated
        /// </summary>
        ORM = 1,

        /// <summary>
        /// Ensure that results are not duplicated. There will only be 1 copy of each row returned
        /// </summary>
        DoNotDuplicate
    }
}
