using System;
using System.Linq;

namespace SqlDsl.Schema
{
    [System.AttributeUsage(System.AttributeTargets.Class | System.AttributeTargets.Struct, Inherited = true, AllowMultiple = false)]
    public class TableAttribute : Attribute
    {
        public readonly string Name;
        public readonly string Schema;

        public TableAttribute(string name)
        {
            Name = name;
        }

        public static string GetTableName(Type tableType)
        {
            return tableType
                .GetCustomAttributes(true)
                .OfType<TableAttribute>()
                .FirstOrDefault()?.Name ?? tableType.Name;
        }
    }
}