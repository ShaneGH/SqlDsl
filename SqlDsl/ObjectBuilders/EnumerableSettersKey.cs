using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A key for a dictionary of enumerable setters
    /// </summary>
    class EnumerableSettersKey : Tuple<Type, string, Type>, IStructuralComparable, IStructuralEquatable, IComparable
    {
        public Type ObjectType => Item1;
        public string PropertyName => Item2;
        public Type ResultPropertyType => Item3;

        public EnumerableSettersKey(Type objectType, string propertyName, Type resultPropertyType)
            : base(objectType, propertyName, resultPropertyType)
        {
        }
    }
}
