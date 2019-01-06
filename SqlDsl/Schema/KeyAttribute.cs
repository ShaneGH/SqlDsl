using System;
using System.Linq;
using System.Reflection;

namespace SqlDsl.Schema
{
    [System.AttributeUsage(System.AttributeTargets.Property | System.AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
    public sealed class KeyAttribute : Attribute
    {
    }
}