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
            
        /// <summary>
        /// A set of generic methods that can be used to reference a single property from a list of items.
        /// </summary>
        static readonly HashSet<MethodInfo> _One = new HashSet<MethodInfo>
        {
            GetMethod(() => Sql.One<object>(null)).GetGenericMethodDefinition(),
            GetMethod(() => Enumerable.First<object>(null)).GetGenericMethodDefinition(),
            GetMethod(() => Enumerable.Single<object>(null)).GetGenericMethodDefinition()
        };

        /// <summary>
        /// If the input expression represents a call to Sql.One([inner expr]), Enumerable.First([inner expr])
        /// or Enumerable.Single([inner expr]), return [inner expr]; otherwise return null
        /// </summary>
        public static Expression IsOne(Expression e)
        {
            var method = RemoveConvert(e) as MethodCallExpression;
            if (method == null || !method.Method.IsGenericMethod)
                return null;

            var methodGeneric = method.Method.GetGenericMethodDefinition();
            if (_One.Contains(methodGeneric))
                return RemoveConvert(method.Arguments[0]);

            return null;
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

        static readonly MethodInfo _Select = GetMethod(() => new object[0].Select(x => x)).GetGenericMethodDefinition();

        /// <summary>
        /// Determine whether an expression is a Select(...) where the mapper is an expression (not a Func)
        /// </summary>
        /// <returns>isSelect: success or failure,
        /// enumerable: the enumerable it maps
        /// mapper: the mapping expression
        /// </returns>
        public static (bool isSelect, Expression enumerable, LambdaExpression mapper) IsSelectWithLambdaExpression(MethodCallExpression e)
        {
            if (!e.Method.IsGenericMethod || e.Method.GetGenericMethodDefinition() != _Select)
                return (false, null, null);

            var mapper = e.Arguments[1] as LambdaExpression;
            if (mapper == null)
                return (false, null, null);

            return (true, e.Arguments[0], mapper);
        }

        static readonly MethodInfo _Joined = GetMethod(() => new object[0].Joined(1)).GetGenericMethodDefinition();

        /// <summary>
        /// Determine whether an expression is a Joined(...).
        /// </summary>
        /// <returns>isJoined: success or failure,
        /// joinedFrom: the joined from table
        /// joinedTo: the table joined to
        /// </returns>
        public static (bool isJoined, Expression joinedFrom, Expression joinedTo) IsJoined(MethodCallExpression e)
        {
            if (!e.Method.IsGenericMethod || e.Method.GetGenericMethodDefinition() != _Joined)
                return (false, null, null);

            return (true, e.Arguments[0], e.Arguments[1]);
        }

        static readonly MethodInfo _ToArray = GetMethod(() => new object[0].ToArray()).GetGenericMethodDefinition();

        /// <summary>
        /// Determine whether an expression is a ToArray().
        /// </summary>
        /// <returns>isToArray: success or failure,
        /// enumerable: the enumerable it converts to an array
        /// </returns>
        public static (bool isToArray, Expression enumerable) IsToArray(MethodCallExpression e)
        {
            if (!e.Method.IsGenericMethod || e.Method.GetGenericMethodDefinition() != _ToArray)
                return (false, null);

            return (true, e.Arguments[0]);
        }

        static readonly MethodInfo _ToList = GetMethod(() => new object[0].ToList()).GetGenericMethodDefinition();

        /// <summary>
        /// Determine whether an expression is a ToList().
        /// </summary>
        /// <returns>isToList: success or failure,
        /// enumerable: the enumerable it converts to a list
        /// </returns>
        public static (bool isToList, Expression enumerable) IsToList(MethodCallExpression e)
        {
            if (!e.Method.IsGenericMethod || e.Method.GetGenericMethodDefinition() != _ToList)
                return (false, null);

            return (true, e.Arguments[0]);
        }
    }
}
