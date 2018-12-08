using System;
using System.Collections.Concurrent;
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
        /// <param name="recurse">If tru, will return the root T of IEnumerable&lt;IEnumerable&lt;T>></param>
        /// <param name="strict">If true, type must be == IEnumerable&lt;T></param>
        public static Type GetIEnumerableType(Type t, bool recurse = false, bool strict = false)
        {
            if (t == typeof(string))
                return null;

            var toTest = strict ?
                t.ToEnumerable() :
                t
                    .GetInterfaces()
                    .Concat(t.IsInterface ? new[] { t } : new Type[0]);

            var result = toTest
                .Where(i => i.IsConstructedGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                .Select(i => i.GetGenericArguments()[0])
                .FirstOrDefault();

            return result != null && recurse ?
                GetIEnumerableType(result) ?? result :
                result;
        }

        static readonly ConcurrentDictionary<Type, int> CountEnumerablesCache = new ConcurrentDictionary<Type, int>();

        /// <summary>
        /// Given a type count how many wrapped IEnumerables it has.
        /// e.g. the type is IEnumerable&lt;IEnumerable&lt;int>> the result will be 2
        /// </summary>
        public static int CountEnumerables(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            
            var key = type;
            if (CountEnumerablesCache.TryGetValue(key, out int value))
                return value;

            value = -1;
            while (type != null)
            {
                value++;
                type = ReflectionUtils.GetIEnumerableType(type);
            }

            return CountEnumerablesCache.GetOrAdd(key, value);
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
        /// The sql IN method.
        /// </summary>
        static readonly MethodInfo _In = GetMethod(() => Sql.In<object>(null, null)).GetGenericMethodDefinition();
            
        /// <summary>
        /// The sql Contains (IN) method.
        /// </summary>
        static readonly MethodInfo _Contains = GetMethod(() => Enumerable.Contains<object>(null, null)).GetGenericMethodDefinition();

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
            if (_In == methodGeneric)
            {
                return (true, RemoveConvert(method.Arguments[0]), RemoveConvert(method.Arguments[1]));
            }

            if (_Contains == methodGeneric)
            {
                return (true, RemoveConvert(method.Arguments[1]), RemoveConvert(method.Arguments[0]));
            }

            return (false, null, null);
        }

        /// <summary>
        /// Returns true if the input expression does not require any parameters to execute
        /// </summary>
        public static bool IsConstant(Expression expr)
        {
            return IsConstant(expr, null).isConstant;
        }

        /// <summary>
        /// Returns true if the input expression does not require any parameters to execute
        /// </summary>
        public static (bool isConstant, bool requiresArgs) IsConstant(Expression expr, ParameterExpression argsParam)
        {
            bool isConstant, requiresArgs;
            switch (expr.NodeType)
            {
                case ExpressionType.NewArrayInit:
                    var ra1 = false;
                    foreach (var el in (expr as NewArrayExpression).Expressions)
                    {
                        (isConstant, requiresArgs) = IsConstant(el, argsParam);
                        if (!isConstant) return (false, false);
                        ra1 |= requiresArgs;
                    }

                    return (true, ra1);

                case ExpressionType.Constant:
                    return (true, false);
                    
                case ExpressionType.MemberAccess:
                    var mem = expr as MemberExpression;
                    if (mem.Expression == null)
                        return (true, false);

                    return IsConstant(mem.Expression, argsParam);

                case ExpressionType.Call:
                    var call = expr as MethodCallExpression;
                    
                    var ra2 = false;
                    if (call.Object != null)
                    {
                        (isConstant, requiresArgs) = IsConstant(call.Object, argsParam);
                        if (!isConstant) return (false, false);
                        ra2 |= requiresArgs;
                    }

                    foreach (var el in call.Arguments)
                    {
                        (isConstant, requiresArgs) = IsConstant(el, argsParam);
                        if (!isConstant) return (false, false);
                        ra2 |= requiresArgs;
                    }

                    return (true, ra2);

                case ExpressionType.Parameter:
                    return (expr == argsParam, true);
                    
                default:
                    return (false, false);
            }
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
        public static (bool isPropertyChain, Expression root, IEnumerable<string> chain) GetPropertyChain(
            Expression e, 
            bool allowOne = false,
            bool allowSelect = false,
            bool allowConstants = false)
        {
            var (isPropertyChain, chains) = GetPropertyChains(e, allowOne, allowSelect, allowConstants, false);
            return isPropertyChain ?
                (true, chains?.First.root, chains?.First.chain) :
                (false, null, null);
        }

        /// <summary>
        /// If an expression is a property chain, return it's root and the property names, otherwise, return false for isPropertyChain
        /// </summary>
        /// <param name="allowOne">If true, calls to .One() will be ignored in the chain</param>
        /// <param name="allowSelect">If true, calls to .Select(...) will be considered part of the the chain if the mapping is also a property chain</param>
        public static (bool isPropertyChain, StructAccumulator<(Expression root, IEnumerable<string> chain, Expression fullExpression), ExpressionType>) GetPropertyChains(
            Expression e, 
            bool allowOne = false,
            bool allowSelect = false,
            bool allowConstants = false)
        {
            var (success, result) = GetPropertyChains(e, allowOne, allowSelect, allowConstants, true);
            return success ?
                (true, new StructAccumulator<(Expression, IEnumerable<string>, Expression), ExpressionType>(result)) :
                (false, null);
        }

        /// <summary>
        /// If an expression is a property chain, return it's root and the property names, otherwise, return false for isPropertyChain
        /// </summary>
        /// <param name="allowOne">If true, calls to .One() will be ignored in the chain</param>
        /// <param name="allowSelect">If true, calls to .Select(...) will be considered part of the the chain if the mapping is also a property chain</param>
        static (bool isPropertyChain, Accumulator<(Expression root, IEnumerable<string> chain, Expression fullExpression), ExpressionType>) GetPropertyChains(
            Expression e, 
            bool allowOne,
            bool allowSelect,
            bool allowConstants,
            bool allowBinaryOperators)
        {
            switch (e.NodeType)
            {
                case ExpressionType.Constant:
                    if (!allowConstants)
                        return (false, null);

                    return (true, new Accumulator<(Expression, IEnumerable<string>, Expression), ExpressionType>((e, Enumerable.Empty<string>(), e)));

                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    if (!allowBinaryOperators)
                        return (false, null);

                    var asBinary = e as BinaryExpression;
                    var pchain1 = GetPropertyChains(asBinary.Left, allowOne, allowSelect, allowConstants, allowBinaryOperators);
                    var pchain2 = GetPropertyChains(asBinary.Right, allowOne, allowSelect, allowConstants, allowBinaryOperators);

                    if (!pchain1.isPropertyChain || !pchain2.isPropertyChain)
                        return (false, null);

                    return (true, pchain1.Item2.Combine(pchain2.Item2, e.NodeType));
                    
                case ExpressionType.Convert:
                    return GetPropertyChains((e as UnaryExpression).Operand, allowOne, allowSelect, allowConstants, allowBinaryOperators)
                        .ReplaceFullExpressionIfOnlyOneItem(e);
                    
                case ExpressionType.MemberAccess:
                    var acc = e as MemberExpression;
                    if (acc.Expression == null)
                    {
                        if (IsConstant(acc))
                        {
                            // TODO: is there a better way of doing this?
                            var value = Expression.Lambda(acc).Compile().DynamicInvoke();
                            return GetPropertyChains(
                                Expression.Constant(value), 
                                allowOne, 
                                allowSelect, 
                                allowConstants, 
                                allowBinaryOperators);
                        }

                        return (false, null);
                    }

                    var (isPropertyChain1, chains1) = GetPropertyChains(acc.Expression, allowOne, allowSelect, allowConstants, allowBinaryOperators);
                    
                    return isPropertyChain1 ?
                        (isPropertyChain1, chains1.Map(c => (c.root, c.chain.Append(acc.Member.Name), e))) :
                        (false, null);
                    
                case ExpressionType.Parameter:
                    return (true, new Accumulator<(Expression, IEnumerable<string>, Expression), ExpressionType>((e, Enumerable.Empty<string>(), e)));
                    
                case ExpressionType.Call:
                    var methodCallE = e as MethodCallExpression;
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(methodCallE);
                    if (isToArray)
                        return GetPropertyChains(enumerableA, allowOne, allowSelect, allowConstants, allowBinaryOperators)
                            .ReplaceFullExpressionIfOnlyOneItem(e);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(methodCallE);
                    if (isToList)
                        return GetPropertyChains(enumerableL, allowOne, allowSelect, allowConstants, allowBinaryOperators)
                            .ReplaceFullExpressionIfOnlyOneItem(e);

                    if (allowOne)
                    {
                        var oneExpr = ReflectionUtils.IsOne(e);
                        if (oneExpr != null)
                        {
                            return GetPropertyChains(oneExpr, allowOne, allowSelect, allowConstants, allowBinaryOperators)
                                .ReplaceFullExpressionIfOnlyOneItem(e);
                        } 
                    }

                    if (allowSelect)
                    {
                        var (isSelect, enumerable, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(methodCallE);
                        if (isSelect)
                        {
                            var (isPropertyChain2, chains2) = GetPropertyChains(enumerable, allowOne, allowSelect, allowConstants, allowBinaryOperators);
                            var (isPropertyChain3, chains3) = GetPropertyChains(mapper.Body, allowOne, allowSelect, allowConstants, allowBinaryOperators);
                            if (!isPropertyChain2 || !isPropertyChain3)
                                return (false, null);

                            if (chains2.Next.Any())
                                return (false, null);

                            return (true, chains3.Map(ch => (chains2.First.root, chains2.First.chain.Concat(ch.chain), e)));
                        } 
                    }
                    
                    return (false, null);
                default:
                    return (false, null);
            }
        }

        static (bool, Accumulator<(Expression, IEnumerable<string>, Expression), ExpressionType>) ReplaceFullExpressionIfOnlyOneItem(this (bool, Accumulator<(Expression, IEnumerable<string>, Expression), ExpressionType>) input, Expression expr)
        {
            if (!input.Item1 || input.Item2.Next.Any()) return (false, null);
            return (true, input.Item2.Map(x => (x.Item1, x.Item2, expr)));
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
