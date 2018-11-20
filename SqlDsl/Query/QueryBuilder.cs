using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    /// <summary>
    /// Object to append query values to via underlying DSL
    /// </summary>
    public partial class QueryBuilder<TSqlBuilder, TArgs, TResult> : ITable<TArgs, TResult>, IQuery<TArgs, TResult>, IOrdererAgain<TArgs, TResult>
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        /// <summary>
        /// The name of the table in the SELECT statement
        /// </summary>
        string PrimaryTableName;
        
        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        public (string name, Type type)? PrimaryTableMember { get; private set; }
        
        /// <summary>
        /// The joins applied to the query
        /// </summary>
        readonly List<Join> Joins = new List<Join>();
        
        /// <summary>
        /// The WHERE part of the query
        /// </summary>
        (ParameterExpression queryRoot, ParameterExpression args, Expression where)? WhereClause = null;

        /// <summary>
        /// Check an expression ultimately points to the query object. Throw an exception if not
        /// </summary>
        static (string name, Type Type) CheckMemberExpression(Expression body, ParameterExpression queryParameter)
        {
            body = ReflectionUtils.RemoveConvert(body);
            if (body == queryParameter)
                return (SqlStatementConstants.RootObjectAlias, queryParameter.Type);

            // build a chain of property names
            var output = new List<MemberInfo>();
            var expr = TryOne(body) as MemberExpression;
            while (expr != null)
            {
                output.Insert(0, expr.Member);
                expr = TryOne(expr.Expression) as MemberExpression;
            }

            if (!output.Any() || output[0].DeclaringType != typeof(TResult))
                throw new ArgumentException("This expression must point to a paramater on the query object.", nameof(body));
                
            // return the name and type
            return (output.MemberName(), output.Last().GetPropertyOrFieldType());

            Expression TryOne(Expression val) => ReflectionUtils.IsOne(val) ?? val;
        }

        /// <summary>
        /// Return all of the property and field names of a type as column names
        /// </summary>
        static IEnumerable<(string name, Type dataType)> GetColumnNames(Type t)
        {
            foreach (var col in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                yield return (col.Name, col.PropertyType);
                
            foreach (var col in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
                yield return (col.Name, col.FieldType);
        }

        /// <summary>
        /// A cache of column names for a given type
        /// </summary>
        static readonly ConcurrentDictionary<Type, IEnumerable<(string name, Type dataType)>> Columns = new ConcurrentDictionary<Type, IEnumerable<(string, Type)>>();
        
        /// <summary>
        /// Get all of the column names for a given type
        /// </summary>
        static IEnumerable<(string name, Type dataType)> ColumnsOf(Type t)
        {
            if (Columns.TryGetValue(t, out IEnumerable<(string, Type)> value))
                return value;

            value = GetColumnNames(t)
                .ToList()
                .AsReadOnly();
                
            return Columns.GetOrAdd(t, value);
        }
    }
}
