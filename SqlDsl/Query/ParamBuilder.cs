using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Query
{
    /// <summary>
    /// An object to record and name parameters
    /// </summary>
    public class ParamBuilder
    {
        static readonly Regex ParamNumberRegex = new Regex(@"^@p(\d+)((" + Regex.Escape(SqlStatementConstants.ParamArrayFlag) + ")?)$");
        readonly IList<(object, Type)> Params = new List<(object, Type)>();

        public IEnumerable<object> Parameters => Params.Select(p => p.Item1);

        public string AddParam(object value, Type paramType)
        {
            lock (Params)
            {
                Params.Add((value, paramType));
                return $"@p{Params.Count - 1}";
            }
        }

        static int GetParameterIndex(string parameterName)
        {
            var match = ParamNumberRegex.Match(parameterName);
            if (!match.Success)
                throw new InvalidOperationException($"Could not find parameter {parameterName}");

            return int.Parse(match.Groups[1].Captures[0].Value);
        }

        public Type GetParameterType(string parameterName)
        {
            var i = GetParameterIndex(parameterName);
            if (Params.Count <= i)
                throw new InvalidOperationException($"Could not find type for parameter {parameterName}");

            return Params[i].Item2;
        }
    }
}
