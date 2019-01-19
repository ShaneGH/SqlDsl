using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Utils
{
    public static class CodingConstants
    {
        public static readonly MethodInfo GenericSelectMethod = ReflectionUtils
            .GetMethod(() => new int[0].Select(x => x))
            .GetGenericMethodDefinition();
        
        public class Empty
        {
            public static readonly (string, object)[] StringObject = Enumerable
                .Empty<(string, object)>()
                .ToArray();
                
            public static readonly ConstructorInfo[] ConstructorInfo = new ConstructorInfo[0];
                
            public static readonly int[] Int = new int[0];
                
            public static readonly IQueryTable[] IQueryTable = new IQueryTable[0];
        }
        
        public class Null
        {
            public static readonly string String = null;
        }
        
        public class Expressions
        {
            public static readonly MethodCallExpression SqlRowNumber = Expression.Call(
                ReflectionUtils.GetMethod(() => Sql.RowNumber()));
        }
    }
}
