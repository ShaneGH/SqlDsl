using System;
using System.Linq;
using System.Reflection;

namespace SqlDsl.Schema
{
    [System.AttributeUsage(System.AttributeTargets.Property | System.AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
    public sealed class KeyAttribute : Attribute
    {
        public readonly int Index;

        public KeyAttribute(int index)
        {
            Index = index;
        }

        public KeyAttribute()
            : this (int.MaxValue)
        {
        }

        public static int? GetKeyIndex(MemberInfo propertyOrField)
        {
            return propertyOrField
                .GetCustomAttributes(true)
                .OfType<KeyAttribute>()
                .FirstOrDefault()?.Index;
        }
    }
}