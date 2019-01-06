using System;
using System.Linq;
using System.Reflection;

namespace SqlDsl.Schema
{
    [System.AttributeUsage(System.AttributeTargets.Property | System.AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
    public sealed class ColumnAttribute : Attribute
    {
        public readonly string Name;

        public ColumnAttribute(string name)
        {
            Name = name;
        }

        public static (string name, string alias) GetColumnName(MemberInfo propertyOrField)
        {
            return (
                propertyOrField
                    .GetCustomAttributes(true)
                    .OfType<ColumnAttribute>()
                    .FirstOrDefault()?.Name ?? propertyOrField.Name,
                propertyOrField.Name);
        }
    }
}