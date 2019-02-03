using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Mapper;

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

            var toTest = strict
                ? t.ToEnumerable()
                : t
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
        
        static readonly MethodInfo RowNumberMethod = GetMethod<int>(x => x.RowNumber()).GetGenericMethodDefinition();
        
        static readonly MethodInfo NullableRowNumberMethod = GetMethod<int>(x => x.NullableRowNumber()).GetGenericMethodDefinition();

        /// <summary>
        /// If the input expression represents a call to Sql.RowNumber() or Sql.NullableRowNumber()
        /// </summary>
        public static (bool isRowNumber, bool isNullable) IsRowNumber(MethodCallExpression e)
        {
            if (!e.Method.IsGenericMethod)
                return (false, false);

            if (e.Method.GetGenericMethodDefinition() == RowNumberMethod)
                return (true, false);

            if (e.Method.GetGenericMethodDefinition() == NullableRowNumberMethod)
                return (true, true);
                
            return (false, false);
        }

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
                    if (IsRowNumber(call).isRowNumber)
                        return (false, false);
                    
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
        /// convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
        /// </summary>
        public static Expression ConvertCollectionToFullMemberInit(Type tMapped, Expression collection)
        {
            var enumeratedType = GetIEnumerableType(tMapped);
            if (enumeratedType == null)
                throw new InvalidOperationException($"Expected type {tMapped} to implement IEnumerable<>");

            var innerParam = Expression.Parameter(enumeratedType);
            var mapperBody = ConvertToFullMemberInit(innerParam);
            var mapper = Expression.Lambda(mapperBody, innerParam);

            return Expression.Call(
                GetMethod<IEnumerable<object>>(xs => xs.Select(x => x), enumeratedType, enumeratedType),
                collection,
                mapper);
        }

        /// <summary>
        /// convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
        /// </summary>
        public static Expression ConvertCollectionToFullMemberInit(Expression collection) => ConvertCollectionToFullMemberInit(collection.Type, collection);

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
            switch (e.NodeType)
            {
                case ExpressionType.Constant:
                    if (!allowConstants)
                        return (false, null, null);

                    return (true, e, Enumerable.Empty<string>());
                    
                case ExpressionType.Convert:
                    return GetPropertyChain((e as UnaryExpression).Operand, allowOne, allowSelect, allowConstants);
                    
                case ExpressionType.MemberAccess:
                    var acc = e as MemberExpression;
                    if (acc.Expression == null)
                    {
                        if (IsConstant(acc))
                        {
                            // TODO: is there a better way of doing this?
                            var value = Expression.Lambda(acc).Compile().DynamicInvoke();
                            return GetPropertyChain(
                                Expression.Constant(value), 
                                allowOne, 
                                allowSelect, 
                                allowConstants);
                        }

                        return (false, null, null);
                    }

                    var (isPropertyChain1, param1, chains1) = GetPropertyChain(acc.Expression, allowOne, allowSelect, allowConstants);
                    
                    return isPropertyChain1 ?
                        (isPropertyChain1, param1, chains1.Append(acc.Member.Name)) :
                        (false, null, null);
                    
                case ExpressionType.Parameter:
                    return (true, e, Enumerable.Empty<string>());
                    
                case ExpressionType.Call:
                    var methodCallE = e as MethodCallExpression;
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(methodCallE);
                    if (isToArray)
                        return GetPropertyChain(enumerableA, allowOne, allowSelect, allowConstants);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(methodCallE);
                    if (isToList)
                        return GetPropertyChain(enumerableL, allowOne, allowSelect, allowConstants);

                    if (allowOne)
                    {
                        var oneExpr = ReflectionUtils.IsOne(e);
                        if (oneExpr != null)
                            return GetPropertyChain(oneExpr, allowOne, allowSelect, allowConstants);
                    }

                    if (allowSelect)
                    {
                        var (isSelect, enumerable, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(methodCallE);
                        if (isSelect)
                        {
                            var (isPropertyChain2, root2, chains2) = GetPropertyChain(enumerable, allowOne, allowSelect, allowConstants);
                            var (isPropertyChain3, root3, chains3) = GetPropertyChain(mapper.Body, allowOne, allowSelect, allowConstants);
                            if (!isPropertyChain2 || !isPropertyChain3)
                                return (false, null, null);

                            return (true, root2, chains2.Concat(chains3));
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

        /// <summary>
        /// Determine whether an expression is a Count().
        /// </summary>
        /// <returns>isCount: success or failure,
        /// enumerable: the enumerable it counts
        /// </returns>
        public static (bool isCount, Expression enumerable) IsCount(Expression e)
        {
            
            if (e is MethodCallExpression)
                return IsCount(e as MethodCallExpression);
            if (e is MemberExpression)
                return IsCount(e as MemberExpression);

            return (false, null);
        }
        
        static readonly HashSet<MethodInfo> CountMethods = new HashSet<MethodInfo>
        {
            GetMethod(() => new object[0].Count()).GetGenericMethodDefinition(),
            GetMethod(() => new object[0].LongCount()).GetGenericMethodDefinition()
        };
        
        static (bool isCount, Expression enumerable) IsCount(MethodCallExpression e)
        {
            if (e.Method.IsGenericMethod && CountMethods.Contains(e.Method.GetGenericMethodDefinition()))
                return (true, e.Arguments[0]);

            if (e.Object != null && e.Object.Type.IsArray &&
                (e.Method.Name == "GetLength" || e.Method.Name == "GetLongLength"))
                return (true, e.Object);

            return (false, null);
        }
        
        static readonly HashSet<Type> CountPropertyTypes = new HashSet<Type>
        {
            typeof(ICollection),
            typeof(ICollection<>),
            typeof(CollectionBase),
            typeof(IList),
            typeof(IList<>),
            typeof(List<>),
            typeof(ISet<>),
            typeof(HashSet<>),
            typeof(IReadOnlyCollection<>),
            typeof(IReadOnlyList<>)
        };
        
        static (bool isCount, Expression enumerable) IsCount(MemberExpression e)
        {
            if (e.Expression == null)
                return (false, null);

            if (e.Expression.Type.IsArray && e.Member.Name == "Length")
                return (true, e.Expression);

            var t = e.Expression.Type.IsGenericType ? e.Expression.Type.GetGenericTypeDefinition() : e.Expression.Type;
            if (CountPropertyTypes.Contains(t) && e.Member.Name == "Count") 
                return (true, e.Expression);

            // // TODO: is this something that I want to support globally?
            // if (CountPropertyTypes.Any(c => c.IsAssignableFrom(t)))
            //     return (true, e.Expression);

            return (false, null);
        }

        static NumberAggregatorTest SumTest = new NumberAggregatorTest("Sum");
        static NumberAggregatorTest AverageTest = new NumberAggregatorTest("Average");
        static NumberAggregatorTest MaxTest = new NumberAggregatorTest("Max");
        static NumberAggregatorTest MinTest = new NumberAggregatorTest("Min");

        /// <summary>
        /// Determine whether an expression is a Sum().
        /// </summary>
        /// <returns>isSum: success or failure,
        /// enumerable: the enumerable it adds
        /// </returns>
        public static (bool isSum, Expression enumerable, LambdaExpression mapper) IsSum(MethodCallExpression e) =>
            SumTest.IsNumberAggregator(e);


        /// <summary>
        /// Determine whether an expression is a Average().
        /// </summary>
        /// <returns>isAverage: success or failure,
        /// enumerable: the enumerable it adds
        /// </returns>
        public static (bool isAverage, Expression enumerable, LambdaExpression mapper) IsAverage(MethodCallExpression e) =>
            AverageTest.IsNumberAggregator(e);


        /// <summary>
        /// Determine whether an expression is a Max().
        /// </summary>
        /// <returns>isMax: success or failure,
        /// enumerable: the enumerable it adds
        /// </returns>
        public static (bool isMax, Expression enumerable, LambdaExpression mapper) IsMax(MethodCallExpression e) =>
            MaxTest.IsNumberAggregator(e);


        /// <summary>
        /// Determine whether an expression is a Min().
        /// </summary>
        /// <returns>isMin: success or failure,
        /// enumerable: the enumerable it adds
        /// </returns>
        public static (bool isMin, Expression enumerable, LambdaExpression mapper) IsMin(MethodCallExpression e) =>
            MinTest.IsNumberAggregator(e);

        /// <summary>
        /// Execute a paramaterless expression like a lambda
        /// </summary>
        public static object ExecuteExpression(Expression e)
        {
            return Expression
                .Lambda<Func<object>>(
                    ReflectionUtils.Convert(
                        e,
                        typeof(object)))
                .Compile()();
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

        static readonly ConcurrentDictionary<Type, MethodInfo> RowNumberMethods = new ConcurrentDictionary<Type, MethodInfo>();
        
        public static MethodInfo GetRowNumberMethod(Type contextType)
        {
            if (!RowNumberMethods.TryGetValue(contextType, out MethodInfo result))
            {
                result = GetMethod<int>(x => x.RowNumber(), contextType);
                RowNumberMethods.TryAdd(contextType, result);
            }

            return result;
        }

        static readonly ConcurrentDictionary<Type, Type> IsPropMapValueCache = new ConcurrentDictionary<Type, Type>();

        /// <summary>
        /// If the input type is a PropMapValue<>, returns it's generic argument, otherwise returns null
        /// </summary>
        public static Type IsPropMapValue(Type input)
        {
            if (!IsPropMapValueCache.TryGetValue(input, out Type value))
            {
                value = input.IsGenericType && input.GetGenericTypeDefinition() == typeof(PropMapValue<>)
                    ? input.GetGenericArguments()[0]
                    : null;

                IsPropMapValueCache.TryAdd(input, value);
            }

            return value;
        }

        static readonly ConcurrentDictionary<Type, Type> CreatePropMapValueCache = new ConcurrentDictionary<Type, Type>();

        /// <summary>
        /// Creates a PropMapValue<T>
        /// </summary>
        public static Type CreatePropMapValue(Type input)
        {
            if (!CreatePropMapValueCache.TryGetValue(input, out Type value))
            {
                value = typeof(PropMapValue<>).MakeGenericType(input);
                CreatePropMapValueCache.TryAdd(input, value);
            }

            return value;
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
