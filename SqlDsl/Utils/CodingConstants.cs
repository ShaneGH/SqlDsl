using System.Linq;
using System.Reflection;

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
        }
    }
}
