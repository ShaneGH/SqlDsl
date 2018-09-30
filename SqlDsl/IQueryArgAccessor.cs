using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders;

namespace SqlDsl
{
    /// <summary>
    /// Get a single value from a query arg
    /// </summary>
    interface IQueryArgAccessor<TArgs>
    {
        object GetArgValue(TArgs args);
    }
}
