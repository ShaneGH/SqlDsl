using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Query
{
    /// <summary>
    /// An object to record and name parameters
    /// </summary>
    public class ParamBuilder
    {
        private readonly IList<object> Params;

        public IEnumerable<object> Parameters => Params.Skip(0);

        public ParamBuilder(IList<object> param)
        {
            Params = param;
        }
        
        public ParamBuilder()
            : this(new List<object>())
        {
        }

        public string AddParam(object value)
        {
            lock (Params)
            {
                Params.Add(value);
                return $"@p{Params.Count - 1}";
            }
        }
    }
}
