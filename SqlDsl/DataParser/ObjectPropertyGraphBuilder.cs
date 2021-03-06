using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Util methods for builing an object property graph
    /// </summary>
    public static class ObjectPropertyGraphBuilder
    {
        static readonly Regex PrimarkKeyColumn = new Regex($@"^{SqlStatementConstants.PrimaryKeyName}\d+$", RegexOptions.Compiled);

        public static RootObjectPropertyGraph Build(
            Type objectType, 
            int[] primaryKeyColumns,
            IEnumerable<(string name, int[] primaryKeyColumnMap)> mappedTableProperties, 
            IEnumerable<(string name, int[] primaryKeyColumnMap, Type cellType, ConstructorInfo[] constructorArgInfo)> columns, 
            QueryParseType queryParseType)
        {
            columns = columns.Enumerate();

            var opg = _Build(
                objectType, 
                primaryKeyColumns,
                mappedTableProperties.Select(c => (c.name.Split('.'), c.primaryKeyColumnMap)),
                columns.OrEmpty().Select((c, i) => (i, c.name.Split('.'), c.primaryKeyColumnMap, c.cellType, c.constructorArgInfo)), 
                queryParseType);

            return new RootObjectPropertyGraph(
                opg.ObjectType,
                columns.Select(x => x.name), 
                opg.SimpleProps, 
                opg.ComplexProps, 
                opg.PrimaryKeyColumns,
                opg.SimpleConstructorArgs,
                opg.ComplexConstructorArgs);
        }

        static void ValidateColumns(IEnumerable<(string name, int[] primaryKeyColumnMap, Type cellType, ConstructorInfo[] constructorArgInfo)> columns)
        {
            foreach(var col in columns)
            {
                var constructorCount = SqlStatementConstants.ConstructorArgs.CountConstructorArgs(col.name);
                if (col.constructorArgInfo.Length != constructorCount)
                    throw new InvalidOperationException($"Expecting {col.constructorArgInfo.Length} constructors, but got {constructorCount}.");
            }
        }

        static readonly Type[] EmptyType = new Type[0];

        static ObjectPropertyGraph _Build(
            Type objectType, 
            int[] primaryKeyColumns, 
            IEnumerable<(string[] name, int[] primaryKeyColumnMap)> mappedTableProperties, 
            IEnumerable<(int index, string[] name, int[] primaryKeyColumnMap, Type cellType, ConstructorInfo[] constructorArgInfo)> columns, 
            QueryParseType queryParseType)
        {
            var simpleProps = new List<SimpleProp>();
            var complexProps = new List<(int index, string propertyName, string[] subPropName, int[] subPropPrimaryKeyColumns, Type propertyType, Type dataCellType, ConstructorInfo[] constructorArgInfo)>();
            var simpleCArgs = new List<SimpleConstructorArg>();
            var complexCArgs = new List<(int index, int argIndex, string[] subPropName, int[] subPropPrimaryKeyColumns, Type propertyType, Type dataCellType, ConstructorInfo[] constructorArgInfo)>();
            
            // if mappedTableProperties are invalid
            // check mapped tables in QueryMapper.BuildMapForSelect(...)
            mappedTableProperties = mappedTableProperties
                .Select(p => (
                    p.name, 
                    RemoveBeforePattern(primaryKeyColumns, p.primaryKeyColumnMap, throwErrorIfPatternNotFound: false)
                ))
                .Enumerate();

            var typedColNames = GetProperties(objectType);
            var typedConstructorArgs = columns
                .Where(c => SqlStatementConstants.ConstructorArgs.IsConstructorArg(c.name[0]))
                .Select(c => c.constructorArgInfo[0].GetParameters().Select(p => p.ParameterType).ToArray())
                .FirstOrDefault() ?? EmptyType;

            foreach (var col in columns)
            {
                // if there is only one name, the property belongs to this object
                if (col.name.Length == 1)
                {                
                    if (SqlStatementConstants.ConstructorArgs.TryGetConstructorArgIndex(col.name[0], out int index))
                    {
                        if (typedConstructorArgs.Length <= index)
                            throw new InvalidOperationException($"Expected constructor with at least {index} arguments.");

                        simpleCArgs.Add(new SimpleConstructorArg(
                            col.index, 
                            index, 
                            FilterPrimaryKeyColumns(
                                RemoveBeforePattern(primaryKeyColumns, col.primaryKeyColumnMap)).ToArray(),
                            typedConstructorArgs[index],
                            col.cellType
                        ));
                    }
                    else
                    {
                        var colType = typedColNames.ContainsKey(col.name[0]) ?
                            typedColNames[col.name[0]] :
                            null;

                        simpleProps.Add(new SimpleProp(
                            col.index, 
                            col.name[0], 
                            FilterPrimaryKeyColumns(
                                RemoveBeforePattern(primaryKeyColumns, col.primaryKeyColumnMap)).ToArray(),
                            colType,
                            col.cellType
                        ));
                    }
                }
                // if there are more than one, the property belongs to a child of this object
                else if (col.name.Length > 1)
                {
                    if (SqlStatementConstants.ConstructorArgs.TryGetConstructorArgIndex(col.name[0], out int index))
                    {                            
                        var colType = 
                            ReflectionUtils.GetIEnumerableType(typedConstructorArgs[index]) ??
                            typedConstructorArgs[index];

                        // separate the property from this object (index == 0) from the properties of
                        // child objects
                        complexCArgs.Add((
                            col.index, 
                            index, 
                            col.name.Skip(1).ToArray(),
                            RemoveBeforePattern(primaryKeyColumns, col.primaryKeyColumnMap),
                            colType,
                            col.cellType,
                            col.constructorArgInfo));
                    }
                    else if (typedColNames.ContainsKey(col.name[0]) 
                        // If a table alias and column alias clash,
                        // The typedColNames check will give a false positive 
                        // and the result will be an attempt to create a complex
                        // object from something like a string or int
                        && !PrimarkKeyColumn.IsMatch(col.name[col.name.Length - 1]))
                    {
                        // unwrap from IEnumerable    
                        var colType = 
                            ReflectionUtils.GetIEnumerableType(typedColNames[col.name[0]]) ?? 
                            typedColNames[col.name[0]];

                        // separate the property from this object (index == 0) from the properties of
                        // child objects
                        complexProps.Add((
                            col.index, 
                            col.name[0], 
                            col.name.Skip(1).ToArray(),
                            RemoveBeforePattern(primaryKeyColumns, col.primaryKeyColumnMap),
                            colType,
                            col.cellType,
                            col.constructorArgInfo));
                    }
                }
            }

            var cplxProps = complexProps
                .GroupBy(PropertyName)
                .Select(BuildComplexProp)
                .Enumerate();

            var cplxCArgs = complexCArgs
                .GroupBy(ArgIndex)
                .Select(BuildComplexCArg)
                .Enumerate();

            return new ObjectPropertyGraph(
                objectType, 
                simpleProps.ToArray(), 
                cplxProps.ToArray(), 
                primaryKeyColumns, 
                simpleCArgs.ToArray(), 
                cplxCArgs.ToArray());

            string PropertyName((int, string propertyName, string[], int[], Type, Type, ConstructorInfo[]) value) => value.propertyName;
            int ArgIndex((int, int argIndex, string[], int[], Type, Type, ConstructorInfo[]) value) => value.argIndex;

            ComplexProp BuildComplexProp(IEnumerable<(int index, string propertyName, string[] subPropName, int[] subPropPrimaryKeyColumns, Type propertyType, Type dataCellType, ConstructorInfo[] constructorArgInfo)> values)
            {
                values = values.Enumerate();
                var propertyName = values.First().propertyName;

                // try to get row ids from property table map
                var propertyTableMap = mappedTableProperties
                    .Where(p => p.name.Length == 1 && p.name[0] == propertyName)
                    .Select(x => x.primaryKeyColumnMap)
                    .FirstOrDefault();

                // if there is no map, use the common root for all properties
                // this will happen when it is not a mapped query, 
                // or for properties not bound to a Select(...) statement in a map
                if (propertyTableMap == null)
                {
                    propertyTableMap = values
                        .Where(x => x.subPropName.Length == 1)
                        .Select(x => x.subPropPrimaryKeyColumns)
                        .OrderedIntersection()
                        .ToArray();
                }

                return new ComplexProp(
                    propertyName,
                    _Build(
                        values.First().propertyType,
                        FilterPrimaryKeyColumns(propertyTableMap).ToArray(),
                        mappedTableProperties.Where(p => p.name.Length > 1).Select(p => (p.name.Skip(1).ToArray(), p.primaryKeyColumnMap)),
                        values.Select(v => (v.index, v.subPropName, v.subPropPrimaryKeyColumns, v.dataCellType, v.constructorArgInfo)),
                        queryParseType));
            }

            ComplexConstructorArg BuildComplexCArg(IEnumerable<(int index, int argIndex, string[] subPropName, int[] subPropPrimaryKeyColumns, Type propertyType, Type dataCellType, ConstructorInfo[] constructorArgInfo)> values)
            {
                values = values.Enumerate();
                var argIndex = values.First().argIndex;
                var constructorInfo = values.First().constructorArgInfo.FirstOrDefault();

                if (constructorInfo == null)
                    throw new InvalidOperationException("A constructor is required for constructor args.");

                // try to get row ids from property table map
                var propertyTableMap = mappedTableProperties
                    .Where(p => p.name.Length == 1 && SqlStatementConstants.ConstructorArgs.BuildConstructorArg(argIndex) == p.name[0])
                    .Select(x => x.primaryKeyColumnMap)
                    .FirstOrDefault();

                //int[] propertyTableMap = null;

                // if there is no map, use the common root for all properties
                // this will happen when it is not a mapped query, 
                // or for properties not bound to a Select(...) statement in a map
                if (propertyTableMap == null)
                {
                    propertyTableMap = values
                        .Where(x => x.subPropName.Length == 1)
                        .Select(x => x.subPropPrimaryKeyColumns)
                        .OrderedIntersection()
                        .ToArray();
                }

                return new ComplexConstructorArg(
                    argIndex,
                    constructorInfo.GetParameters()[argIndex].ParameterType,
                    _Build(
                        values.First().propertyType,
                        FilterPrimaryKeyColumns(propertyTableMap).ToArray(),
                        mappedTableProperties.Where(p => p.name.Length > 1).Select(p => (p.name.Skip(1).ToArray(), p.primaryKeyColumnMap)),
                        values.Select(v => (v.index, v.subPropName, v.subPropPrimaryKeyColumns, v.dataCellType, v.constructorArgInfo.Skip(1).ToArray())),
                        queryParseType));
            }

            IEnumerable<int> FilterPrimaryKeyColumns(IEnumerable<int> numbers)
            {
                switch (queryParseType)
                {
                    case QueryParseType.ORM:
                        return numbers;
                    case QueryParseType.DoNotDuplicate:
                        return numbers.Any() ? numbers.Last().ToEnumerableStruct() : numbers;
                    default:
                        throw new NotImplementedException($"QueryParseType \"[{queryParseType}]\" is not supported");
                }
            }
        }

        static IEnumerable<T> OrderedIntersection<T>(this IEnumerable<IEnumerable<T>> items)
            where T: struct // T: struct is shortcut as T will always be int. If using with classes, write code to check for nulls
        {
            items = items.Enumerate();
            if (!items.Any()) return Enumerable.Empty<T>();

            return items.Aggregate(Intersect);

            IEnumerable<T> Intersect(IEnumerable<T> x, IEnumerable<T> y)
            {
                using (var enumX = x.GetEnumerator())
                {
                    using (var enumY = y.GetEnumerator())
                    {
                        while (enumX.MoveNext() && 
                            enumY.MoveNext() && 
                            enumX.Current.Equals(enumY.Current))
                        {
                            yield return enumX.Current;
                        }
                    }
                }
            }
        }

        static readonly int[] EmptyInt = new int[0];

        /// <summary>
        /// Remove all elements from an array which occur before or in a pattern. Throws exception if pattern not found
        /// </summary>
        /// <param name="pattern">The common pattern</param>
        /// <param name="array">The array to trim</param>
        /// <param name="throwErrorIfPatternNotFound">If true, will throw an error if the array does not begin with the pattern</param>
        static int[] RemoveBeforePattern(int[] pattern, int[] array, bool throwErrorIfPatternNotFound = true)
        {
            if (pattern.Length == 0)
                return array;

            int pI = 0, aI = 0;
            for (; pI < pattern.Length && aI < array.Length; aI++)
            {
                if (pattern[pI] == array[aI])
                {
                    pI++;
                    if (pI == pattern.Length)
                    {
                        aI++;
                        break;
                    }
                }
            }

            if (pI != pattern.Length)
            {
                // The pattern is longer than the array, and 
                // the array is fully contained in the pattern.
                // this will happen when referenceing a parnt object in a
                // child map
                if (aI == array.Length)
                {
                    return EmptyInt;
                }

                if (throwErrorIfPatternNotFound)
                {
                    throw new InvalidOperationException($"Could not find pattern in array" + 
                        $"\npattern: [{pattern.JoinString(",")}]\narray: [{array.JoinString(",")}]");
                }
            }

            return array
                .Skip(aI)
                .ToArray();
        }

        static ConcurrentDictionary<Type, Dictionary<string, Type>> PropertyCache = new ConcurrentDictionary<Type, Dictionary<string, Type>>();
        static Dictionary<string, Type> GetProperties(Type objectType)
        {
            if (PropertyCache.TryGetValue(objectType, out Dictionary<string, Type> result))
                return result;

            return PropertyCache.GetOrAdd(
                objectType,
                BuildProperties(objectType));
        }

        static Dictionary<string, Type> BuildProperties(Type objectType) => objectType
            .GetFieldsAndProperties()
            .ToDictionary(x => x.name, x => x.type);
    }
}