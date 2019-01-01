using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Utils
{
    public class NumberAggregatorTest
    {
        readonly HashSet<MethodInfo> PureMethods;
        readonly HashSet<MethodInfo> MappedMethods;

        public NumberAggregatorTest(IEnumerable<MethodInfo> pureMethods, IEnumerable<MethodInfo> mappedMethods)
        {
            PureMethods = pureMethods?.ToHashSet() ?? throw new ArgumentNullException(nameof(pureMethods));
            MappedMethods = mappedMethods?.ToHashSet() ?? throw new ArgumentNullException(nameof(mappedMethods));
        }

        NumberAggregatorTest((IEnumerable<MethodInfo> pureMethods, IEnumerable<MethodInfo> mappedMethods) x)
            : this(x.pureMethods, x.mappedMethods)
        {
        }

        public NumberAggregatorTest(string methoddName)
            : this(BuildConstructorArgs(methoddName))
        {
        }

        static (IEnumerable<MethodInfo>, IEnumerable<MethodInfo>) BuildConstructorArgs(string methoddName)
        {
            var methods = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Where(m => m.Name == methoddName);

            var pure = new List<MethodInfo>();
            var mapped = new List<MethodInfo>();

            foreach (var method in methods)
            {
                var param = method.GetParameters().Length;
                if (param == 1)
                    pure.Add(method);
                if (param == 2)
                    mapped.Add(method);
            }

            return (pure, mapped);
        }

        public (bool isAgg, Expression enumerable, LambdaExpression mapper) IsNumberAggregator(MethodCallExpression e)
        {
            var method = e.Method.IsGenericMethod
                ? e.Method.GetGenericMethodDefinition()
                : e.Method;

            if (PureMethods.Contains(method))
                return (true, e.Arguments[0], null);

            if (MappedMethods.Contains(method))
            {
                if (e.Arguments[1] is LambdaExpression)
                    return (true, e.Arguments[0], e.Arguments[1] as LambdaExpression);

                if (!ReflectionUtils.IsConstant(e.Arguments[1]))
                    return (false, null, null);

                var val = ReflectionUtils.ExecuteExpression(e.Arguments[1]);
                if (!(val is LambdaExpression))
                    return (false, null, null);
                
                return (true, e.Arguments[0], val as LambdaExpression);
            }

            return (false, null, null);
        }
    }
}