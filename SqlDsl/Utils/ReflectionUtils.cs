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
        /// Get the MethodInfo object from a method call. If call is Do&lt;T>(...) and replaceGenerics == [U], will replace T with U
        /// </summary>
        public static MethodInfo GetMethod<TRoot>(Expression<Action<TRoot>> methodCall, params Type[] replaceGenerics) =>
            GetMethod(methodCall.Body, replaceGenerics);
               
        /// <summary>
        /// Get the MethodInfo object from a method call. If call is Do&lt;T>(...) and replaceGenerics == [U], will replace T with U
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

        static readonly Type _Nullable = typeof(Nullable<>);

        /// <summary>
        /// Determine if a type is a Nullable&lt;T>
        /// </summary>
        public static bool IsNullable(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == _Nullable;
        }

        /// <summary>
        /// Get the T from an IEnumerable&lt;T> type, or null if the type is not IEnumerable.
        /// In this case System.String is not considered to be an IEnumerable
        /// </summary>
        public static Type GetIEnumerableType(Type t, bool recurse = false)
        {
            if (t == typeof(string))
                return null;

            var result = t
                .GetInterfaces()
                .Concat(t.IsInterface ? new[] { t } : new Type[0])
                .Where(i => i.IsConstructedGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                .Select(i => i.GetGenericArguments()[0])
                .FirstOrDefault();

            return result != null && recurse ?
                GetIEnumerableType(result) ?? result :
                result;
        }

        /// <summary>
        /// Given a type count how many wrapped IEnumerables it has.
        /// e.g. the type is IEnumerable&lt;IEnumerable&lt;int>> the result will be 2
        /// </summary>
        public static int CountEnumerables(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            var result = -1;
            while (type != null)
            {
                result++;
                type = ReflectionUtils.GetIEnumerableType(type);
            }

            return result;
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
            GetMethod(() => Enumerable.FirstOrDefault<object>(null)).GetGenericMethodDefinition(),
            GetMethod(() => Enumerable.Single<object>(null)).GetGenericMethodDefinition(),
            GetMethod(() => Enumerable.SingleOrDefault<object>(null)).GetGenericMethodDefinition()
        };

        /// <summary>
        /// Get a real value from an expression
        /// </summary>
        /// <returns>memberStaticValue: null if memberHasStaticValue == false
        /// </returns>
        public static (bool memberHasStaticValue, object memberStaticValue) GetExpressionStaticObjectValue(Expression expr)
        {
            // drill down into the member until we get to the root
            if (MemberHasStaticValue(expr))
            {
                // compile the expression
                var valueGetter = Expression
                    .Lambda<Func<object>>(
                        Convert(
                            expr, 
                            typeof(object)))
                    .Compile();

                // get the expression value
                return (true, valueGetter());
            }

            return (false, null);
        }

        /// <summary>
        /// Determine if a real value can be taken from an expression
        /// </summary>
        static bool MemberHasStaticValue(Expression e)
        {
            switch (e.NodeType)
            {
                case ExpressionType.Constant:
                    return true;
                case ExpressionType.MemberAccess:
                    var e1 = (e as MemberExpression).Expression;
                    return e1 == null || MemberHasStaticValue(e1);
                case ExpressionType.Call:
                    var e2 = e as MethodCallExpression;
                    return (e2.Object == null || MemberHasStaticValue(e2.Object)) &&
                        e2.Arguments.All(MemberHasStaticValue);
                default:
                    if (e is UnaryExpression)
                    {
                        var e3 = e as UnaryExpression;
                        return MemberHasStaticValue(e3.Operand);
                    }
                    
                    if (e is BinaryExpression)
                    {
                        var e4 = e as BinaryExpression;
                        return MemberHasStaticValue(e4.Left) && MemberHasStaticValue(e4.Right);
                    }

                    return false;
            }
        }

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
        /// A set of generic methods that can be used to represent a sql IN.
        /// </summary>
        static readonly HashSet<MethodInfo> _In = new HashSet<MethodInfo>
        {
            // indexes are important in this hash set
            GetMethod(() => Sql.In<object>(null, null)).GetGenericMethodDefinition(),
            GetMethod(() => Enumerable.Contains<object>(null, null)).GetGenericMethodDefinition()
        };

        /// <summary>
        /// If the input expression represents a call to Sql.In([inner expr]), Enumerable.Contains([inner expr])
        /// or return [success, lhs (val), rhs (collection)]; otherwise return (false, null, null)
        /// </summary>
        public static (bool isIn, Expression lhs, Expression rhs) IsIn(Expression e)
        {
            var method = RemoveConvert(e) as MethodCallExpression;
            if (method == null || !method.Method.IsGenericMethod)
                return (false, null, null);

            var methodGeneric = method.Method.GetGenericMethodDefinition();
            var methodIndex = _In.IndexOf(methodGeneric);
            if (methodIndex == 0)
            {
                return (true, RemoveConvert(method.Arguments[0]), RemoveConvert(method.Arguments[1]));
            }

            if (methodIndex == 1)
            {
                return (true, RemoveConvert(method.Arguments[1]), RemoveConvert(method.Arguments[0]));
            }

            return (false, null, null);
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

        /// <summary>
        /// Convert an "a => a.b" to "a => new b { b1 = a.b.b1, b2 = a.b.b2 ... }". Also works for enumerables
        /// </summary>
        public static Expression ConvertToFullMemberInit(Expression original)
        {
            var enumeratedType = ReflectionUtils.GetIEnumerableType(original.Type);
            if (enumeratedType != null)
                return ConvertEnumerableToFullMemberInit(original, enumeratedType);

            var constructor = original.Type.GetConstructor(new Type[0]);
            if (constructor == null)
            {
                throw new InvalidOperationException($"Type {original.Type.FullName} does not have a default constructor.");
            }

            return Expression.MemberInit(
                Expression.New(constructor),
                ReflectionUtils
                    .GetFieldAndPropertyMembers(original.Type)
                    .Where(x => !x.isReadonly)
                    .Select(m => Expression.Bind(m.Item1, Expression.PropertyOrField(original, m.Item1.Name))));
        }

        /// <summary>
        /// Convert an "a => a.bs" to "a => a.bs.Select(b => new b { b1 = a.b.b1, b2 = a.b.b2 ... })".
        /// </summary>
        static Expression ConvertEnumerableToFullMemberInit(Expression original, Type enumeratedType)
        {
            var mapperInput = Expression.Parameter(enumeratedType);
            var mapper = ConvertToFullMemberInit(mapperInput);
            return Expression.Call(
                ReflectionUtils.GetMethod(() => Enumerable.Select<object, object>(null, x => x), enumeratedType, enumeratedType),
                original,
                Expression.Lambda(mapper, mapperInput));
        }

        /// <summary>
        /// Given a type and chain of properties, get the type of the last prop in the chain.
        /// If any if the props in the middle are IEnumerable, their enumerated type is used
        /// </summary>
        public static (bool chainIsValid, Type type) GetTypeForPropertyChain(Type root, IEnumerable<string> chain)
        {
            chain = chain.Enumerate();
            if (!chain.Any())
                return (true, root);

            root = GetIEnumerableType(root) ?? root;
            var current = chain.First();
            foreach (var prop in GetFieldsAndProperties(root))
            {
                if (prop.name == current)
                    return GetTypeForPropertyChain(prop.type, chain.Skip(1));
            }
            
            return (false, null);
        }

        /// <summary>
        /// If an expression is a property chain, return it's root and the property names, otherwise, return false for isPropertyChain
        /// </summary>
        /// <param name="allowOne">If true, calls to .One() will be ignored in the chain</param>
        /// <param name="allowSelect">If true, calls to .Select(...) will be considered part of the the chain if the mapping is also a property chain</param>
        public static (bool isPropertyChain, ParameterExpression root, IEnumerable<string> chain) GetPropertyChain(
            Expression e, 
            bool allowOne = false,
            bool allowSelect = false)
        {
            switch (e.NodeType)
            {
                case ExpressionType.Convert:
                    return GetPropertyChain((e as UnaryExpression).Operand, allowOne: allowOne, allowSelect: allowSelect);
                    
                case ExpressionType.MemberAccess:
                    var acc = e as MemberExpression;
                    var (isPropertyChain1, root1, chain1) = GetPropertyChain(acc.Expression, allowOne: allowOne, allowSelect: allowSelect);
                    return (isPropertyChain1, root1, chain1.Append(acc.Member.Name));
                    
                case ExpressionType.Parameter:
                    return (true, e as ParameterExpression, Enumerable.Empty<string>());
                    
                case ExpressionType.Call:
                    var methodCallE = e as MethodCallExpression;
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(methodCallE);
                    if (isToArray)
                        return GetPropertyChain(enumerableA, allowOne: allowOne, allowSelect: allowSelect);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(methodCallE);
                    if (isToList)
                        return GetPropertyChain(enumerableL, allowOne: allowOne, allowSelect: allowSelect);

                    if (allowOne)
                    {
                        var oneExpr = ReflectionUtils.IsOne(e);
                        if (oneExpr != null)
                        {
                            return GetPropertyChain(oneExpr, allowOne: allowOne, allowSelect: allowSelect);
                        } 
                    }

                    if (allowSelect)
                    {
                        var (isSelect, enumerable, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(methodCallE);
                        if (isSelect)
                        {
                            var (isPropertyChain2, root2, chain2) = GetPropertyChain(enumerable, allowOne: allowOne, allowSelect: allowSelect);
                            var (isPropertyChain3, _, chain3) = GetPropertyChain(mapper.Body, allowOne: allowOne, allowSelect: allowSelect);
                            if (isPropertyChain2 && isPropertyChain3)
                            {
                                return (true, root2, chain2.Concat(chain3));
                            }
                        } 
                    }
                    
                    return (false, null, null);
                default:
                    return (false, null, null);
            }
        }

        static readonly MethodInfo IEnumerableToArray = GetMethod(() => new object[0].ToArray()).GetGenericMethodDefinition();

        /// <summary>
        /// Determine whether an expression is a ToArray().
        /// </summary>
        /// <returns>isToArray: success or failure,
        /// enumerable: the enumerable it converts to an array
        /// </returns>
        public static (bool isToArray, Expression enumerable) IsToArray(MethodCallExpression e)
        {
            if (e.Method.IsGenericMethod && e.Method.GetGenericMethodDefinition() == IEnumerableToArray)
                return (true, e.Arguments[0]);

            if (e.Method.DeclaringType.IsGenericType && 
                e.Method.DeclaringType.GetGenericTypeDefinition() == typeof(List<>) &&
                e.Method.Name == "ToArray")
                return (true, e.Object);

            return (false, null);
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

        /// <summary>
        /// Get the public instance fields and properties from a class
        /// </summary>
        public static IEnumerable<(string name, Type type, bool readOnly)> GetFieldsAndProperties(this Type objectType)
        {
            return objectType
                .GetFields(BindingFlags.Public | BindingFlags.Instance)
                .Select(f => (name: f.Name, type: f.FieldType, readOnly: f.IsInitOnly))
                .Concat(objectType
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Select(p => (name: p.Name, type: p.PropertyType, readOnly: p.GetSetMethod() == null)));
        }

        /// <summary>
        /// Get the public instance fields and properties from a class
        /// </summary>
        public static IEnumerable<(MemberInfo, bool isReadonly)> GetFieldAndPropertyMembers(this Type objectType) => objectType
            .GetFields(BindingFlags.Public | BindingFlags.Instance)
            .Select(f => (f as MemberInfo, f.IsInitOnly))
            .Concat(objectType
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Select(p => (p as MemberInfo, p.GetSetMethod() == null)));

        /// <summary>
        /// Get the type for a PropertyInfo or FieldInfo. Throw an exception otherwise
        /// </summary>
        public static Type GetPropertyOrFieldType(this MemberInfo member)
        {
            if (member is PropertyInfo)
                return (member as PropertyInfo).PropertyType;
            if (member is FieldInfo)
                return (member as FieldInfo).FieldType;

            throw new InvalidOperationException("Member must be a property or field: " + member);
        }

        /// <summary>
        /// Proxy to Expression.Convert. This function is convenient for debugging as it can optionally add console.log statements
        /// </summary>
        public static Expression Convert(Expression from, Type t)
        {
            // Func<Expression, Expression, Expression> stringConcat = (x, y) =>
            //     Expression.Call(
            //         GetMethod(() => string.Concat("", "")),
            //         x,
            //         y);

            // var fromTypeName = Expression.Call(
            //     GetMethod(() => GetTypeString(null)),
            //     Expression.Convert(from, typeof(object)));

            // var message = stringConcat(
            //     Expression.Constant("Attempting to cast from ["),
            //     stringConcat(
            //         fromTypeName,
            //         Expression.Constant($"] to [{t.FullName}].")));

            // var log = Expression.Call(
            //     GetMethod(() => Console.WriteLine("")),
            //     message);

            // return Expression.Block(log, Expression.Convert(from, t));

            return Expression.Convert(from, t);
        }

        public static string GetTypeString(object obj)
        {
            if (obj == null) return "null";
            return obj.GetType().FullName;
        }
    }
}
