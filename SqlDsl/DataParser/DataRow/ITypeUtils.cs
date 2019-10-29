using System;
using System.Reflection;

namespace SqlDsl.DataParser.DataRow
{
    public interface ITypeUtils
    {
        Type Type { get; }
        MethodInfo GetMethod { get; }
        MethodInfo AreEqualMethod { get; }
    }
}