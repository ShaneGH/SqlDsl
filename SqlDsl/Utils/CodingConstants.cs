using System;
using System.Linq;
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
            public static readonly (string, object)[] StringObject = Array.Empty<(string, object)>();
                
            public static readonly string[] String = Array.Empty<string>();
                
            public static readonly ConstructorInfo[] ConstructorInfo = Array.Empty<ConstructorInfo>();
                
            public static readonly int[] Int = Array.Empty<int>();
                
            public static readonly IQueryTable[] IQueryTable = Array.Empty<IQueryTable>();

            internal static readonly MappedTable[] MappedTable = Array.Empty<MappedTable>();

            internal static readonly (StringBasedMappedProperty, StringBasedMappedProperty)[] Case = Array.Empty<(StringBasedMappedProperty, StringBasedMappedProperty)>();
        }
        
        public class Null
        {
            public static readonly string String = null;
        }
    }
}
