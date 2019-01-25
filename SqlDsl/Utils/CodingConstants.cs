using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Mapper;
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

            internal static readonly MappedTable[] MappedTable = new MappedTable[0];

            internal static readonly (StringBasedMappedProperty, StringBasedMappedProperty)[] Case = new (StringBasedMappedProperty, StringBasedMappedProperty)[0];
        }
        
        public class Null
        {
            public static readonly string String = null;
        }
    }
}
