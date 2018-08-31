using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Utils
{
    /// <summary>
    /// Extension methods for Reflection
    /// </summary>
    public static class ReflectionUtils
    {   
        /// <summary>
        /// Get the MethodInfo object from a method call. If call is Do<T>(...) and replaceGenerics == [U], will replace T with U
        /// </summary>
        public static MethodInfo GetMethod<TRoot>(Expression<Action<TRoot>> methodCall, params Type[] replaceGenerics) =>
            GetMethod(methodCall.Body, replaceGenerics);
               
        /// <summary>
        /// Get the MethodInfo object from a method call. If call is Do<T>(...) and replaceGenerics == [U], will replace T with U
        /// </summary>
        public static MethodInfo GetMethod(Expression<Action> methodCall, params Type[] replaceGenerics) =>
            GetMethod(methodCall.Body, replaceGenerics);
        
        /// <summary>
        /// Get the MethodInfo object from a method call.
        /// </summary>
        static MethodInfo GetMethod(Expression methodCallBody, params Type[] replaceGenerics)
        {
            // ignore implicit/explicit casting
            while (methodCallBody.NodeType == ExpressionType.Convert)
                methodCallBody = (methodCallBody as UnaryExpression).Operand;

            // check for method
            if (methodCallBody.NodeType != ExpressionType.Call)
                throw new InvalidOperationException($"Input expression must be a method call: {methodCallBody}");

            // return method or new generic method
            var method = (methodCallBody as MethodCallExpression).Method;
            return replaceGenerics == null || replaceGenerics.Length == 0 ?
                method :
                ReplaceMethodGenerics(method, replaceGenerics);
        }

        /// <summary>
        /// Replace method generics
        /// </summary>
        static MethodInfo ReplaceMethodGenerics(MethodInfo method, Type[] generics)
        {
            if (!method.IsGenericMethod || method.GetGenericArguments().Length != generics.Length)
                throw new InvalidOperationException($"Input expression must be a generic method call with {generics.Length}.");

            return method
                .GetGenericMethodDefinition()
                .MakeGenericMethod(generics);
        }

        /// <summary>
        /// Get the T from an IEnumerable<T> type, or null if the type is not IEnumerable
        /// </summary>
        public static Type GetIEnumerableType(Type t)
        {
            if (t == typeof(string))
                return null;

            return t
                .GetInterfaces()
                .Concat(t.IsInterface ? new[] { t } : new Type[0])
                .Where(i => i.IsConstructedGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                .Select(i => i.GetGenericArguments()[0])
                .FirstOrDefault();
        }

        /// <summary>
        /// Joins a group of member names using "." as a delimiter
        /// </summary>
        public static string MemberName(this IEnumerable<MemberInfo> members) => members
            .OrEmpty()
            .Select(m => m.Name)
            .JoinString(".");

            
        static readonly MethodInfo _One = GetMethod(() => Sql.One<object>(null)).GetGenericMethodDefinition();

        /// <summary>
        /// If the input expression represents a call to Sql.One([inner expr]), return [inner expr], otherwise return null
        /// </summary>
        public static Expression IsOne(Expression e)
        {
            var method = RemoveConvert(e) as MethodCallExpression;
            if (method == null)
                return null;

            if (!method.Method.IsGenericMethod ||
                method.Method.GetGenericMethodDefinition() != _One)
                return null;

            return RemoveConvert(method.Arguments[0]);
        }

        /// <summary>
        /// If the input expression is an explicit or implicit cast, strip the cast expression off
        /// </summary>
        public static Expression RemoveConvert(Expression e)
        {
            while (e.NodeType == ExpressionType.Convert)
                e = (e as UnaryExpression).Operand;
                
            return e;
        }
    }
}
