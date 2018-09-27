using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    //TODO: this class needs a cleanup

    /// <summary>
    /// Represents an object with it's property names
    /// </summary>
    public static class OPG
    {
        public static RootObjectPropertyGraph Build(Type objectType, IEnumerable<(string name, int[] rowIdColumnMap)> columns, QueryParseType queryParseType)
        {
            var opg = _Build(
                objectType, 
                new [] { 0 },
                columns.Select((c, i) => (i, c.name.Split('.'), c.rowIdColumnMap)), 
                queryParseType);

            return new RootObjectPropertyGraph(
                columns.Select(x => x.name), 
                opg.SimpleProps, 
                opg.ComplexProps, 
                opg.RowIdColumnNumbers);
        }

        static ObjectPropertyGraph _Build(Type objectType, int[] rowIdColumnNumbers, IEnumerable<(int index, string[] name, int[] rowIdColumnMap)> columns, QueryParseType queryParseType)
        {
            // TODO: rowIdColumnNumbers should be int[]
            var simpleProps = new List<(int index, string propertyName, IEnumerable<int> rowIdColumnNumbers)>();
            var complexProps = new List<(int index, string propertyName, string[] subPropName, int[] subPropRowIdColumnNumbers, Type propertyType)>();

            var typedColNames = GetProperties(objectType);
            foreach (var col in columns)
            {           
                // if there is only one name, the property belongs to this object
                if (col.name.Length == 1)
                {
                    simpleProps.Add((
                        col.index, 
                        col.name[0], 
                        FilterRowIdColumnNumbers(
                            RemoveBeforePattern(rowIdColumnNumbers, col.rowIdColumnMap))
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
                        colType));
                }
            }

            var cplxProps = complexProps
                .GroupBy(PropertyName)
                .Select(BuildComplexProp)
                .Enumerate();

            return new ObjectPropertyGraph(simpleProps, cplxProps, rowIdColumnNumbers);

            string PropertyName((int index, string propertyName, string[] subPropName, int[] subPropRowIdColumnNumbers, Type propertyType) value) => value.propertyName;

            (string, ObjectPropertyGraph) BuildComplexProp(IEnumerable<(int index, string propertyName, string[] subPropName, int[] subPropRowIdColumnNumbers, Type propertyType)> values)
            {
                values = values.Enumerate();
                var ridcn = values
                    .Select(x => x.subPropRowIdColumnNumbers)
                    .OrderedIntersection()
                    .Enumerate();

                return (
                    values.First().propertyName,
                    _Build(
                        values.First().propertyType,
                        FilterRowIdColumnNumbers(ridcn).ToArray(),
                        values.Select(v => (v.index, v.subPropName, v.subPropRowIdColumnNumbers)),
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
        static int[] RemoveBeforePattern(int[] pattern, int[] array)
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
            .GetFields()
            .Select(f => (name: f.Name, type: f.FieldType))
            .Concat(objectType
                .GetProperties()
                .Select(f => (name: f.Name, type: f.PropertyType)))
            .ToDictionary(x => x.name, x => x.type);
    }
}