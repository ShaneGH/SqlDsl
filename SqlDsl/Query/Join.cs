using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Query
{
    public class Join
    {
        public JoinType JoinType { get; set; }
        public string TableName { get; set; }
        public (ParameterExpression rootObjectParam, ParameterExpression joinParam, Expression joinExpression) JoinExpression { get; set; }
        public IEnumerable<MemberInfo> JoinResult { get; set; }
    }
    
    public enum JoinType
    {
        Inner = 1,
        Left
    }
}
