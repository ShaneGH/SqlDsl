using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Util methods for builing an object property graph
    /// </summary>
    public static class ObjectPropertyGraphBuilder
    {
        public static RootObjectPropertyGraph Build(
            Type objectType, 
            IEnumerable<(string name, int[] rowIdColumnMap)> mappedTableProperties, 
            IEnumerable<(string name, int[] rowIdColumnMap, Type cellType)> columns, 
            QueryParseType queryParseType)
        {
            columns = columns.Enumerate();

            var opg = _Build(
                objectType, 
                new [] { 0 },
                mappedTableProperties.Select(c => (c.name.Split('.'), c.rowIdColumnMap)),
                columns.Select((c, i) => (i, c.name.Split('.'), c.rowIdColumnMap, c.cellType)), 
                queryParseType);

            return new RootObjectPropertyGraph(
                columns.Select(x => x.name), 
                opg.SimpleProps, 
                opg.ComplexProps, 
                opg.RowIdColumnNumbers);
        }

        static ObjectPropertyGraph _Build(
            Type objectType, 
            int[] rowIdColumnNumbers, 
            IEnumerable<(string[] name, int[] rowIdColumnMap)> mappedTableProperties, 
            IEnumerable<(int index, string[] name, int[] rowIdColumnMap, Type cellType)> columns, 
            QueryParseType queryParseType)
        {
            // TODO: rowIdColumnNumbers should be int[]
            var simpleProps = new List<(int index, string propertyName, IEnumerable<int> rowIdColumnNumbers, Type resultPropertyType, Type dataCellType)>();
            var complexProps = new List<(int index, string propertyName, string[] subPropName, int[] subPropRowIdColumnNumbers, Type propertyType, Type dataCellType)>();

            mappedTableProperties = mappedTableProperties
                .Select(p => (
                    p.name, 
                    RemoveBeforePattern(rowIdColumnNumbers, p.rowIdColumnMap, throwErrorIfPatternNotFound: false)
                ))
                .Enumerate();

            var typedColNames = GetProperties(objectType);
            foreach (var col in columns)
            {
                // if there is only one name, the property belongs to this object
                if (col.name.Length == 1)
                {
                    var colType = typedColNames.ContainsKey(col.name[0]) ?
                        typedColNames[col.name[0]] :
                        null;

                    simpleProps.Add((
                        col.index, 
                        col.name[0], 
                        FilterRowIdColumnNumbers(
                            RemoveBeforePattern(rowIdColumnNumbers, col.rowIdColumnMap)),
                        colType,
                        col.cellType
                    ));
                }
                // if there are more than one, the property belongs to a child of this object
                else if (col.name.Length > 1 && typedColNames.ContainsKey(col.name[0]))
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
                        RemoveBeforePattern(rowIdColumnNumbers, col.rowIdColumnMap),
                        colType,
                        col.cellType));
                }
            }

            var cplxProps = complexProps
                .GroupBy(PropertyName)
                .Select(BuildComplexProp)
                .Enumerate();

            return new ObjectPropertyGraph(simpleProps, cplxProps, rowIdColumnNumbers);

            string PropertyName((int index, string propertyName, string[] subPropName, int[] subPropRowIdColumnNumbers, Type propertyType, Type dataCellType) value) => value.propertyName;

            (string, ObjectPropertyGraph) BuildComplexProp(IEnumerable<(int index, string propertyName, string[] subPropName, int[] subPropRowIdColumnNumbers, Type propertyType, Type dataCellType)> values)
            {
                values = values.Enumerate();
                var propertyName = values.First().propertyName;

                // try to get row ids from property table map
                var propertyTableMap = mappedTableProperties
                    .Where(p => p.name.Length == 1 && p.name[0] == propertyName)
                    .Select(x => x.rowIdColumnMap)
                    .FirstOrDefault();

                // if there is no map, use the common root for all properties
                // this will happen when it is not a mapped query, 
                // or for properties not bound to a Select(...) statement in a map
                if (propertyTableMap == null)
                {
                    propertyTableMap = values
                        .Where(x => x.subPropName.Length == 1)
                        .Select(x => x.subPropRowIdColumnNumbers)
                        .OrderedIntersection()
                        .ToArray();
                }

                return (
                    propertyName,
                    _Build(
                        values.First().propertyType,
                        FilterRowIdColumnNumbers(propertyTableMap).ToArray(),
                        mappedTableProperties.Where(p => p.name.Length > 1).Select(p => (p.name.Skip(1).ToArray(), p.rowIdColumnMap)),
                        values.Select(v => (v.index, v.subPropName, v.subPropRowIdColumnNumbers, v.dataCellType)),
                        queryParseType));
            }

            IEnumerable<int> FilterRowIdColumnNumbers(IEnumerable<int> numbers)
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

            if (throwErrorIfPatternNotFound && pI != pattern.Length)
            {
                throw new InvalidOperationException($"Could not find pattern in array" + 
                    $"\npattern: [{pattern.JoinString(",")}]\narray: [{array.JoinString(",")}]");
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