using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.DataParser;
using SqlDsl.ObjectBuilders;

namespace SqlDsl.UnitTests.ObjectBuilders
{
    public class TestObjectGraph : ReusableObjectGraph
    {
        public IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleProps;
        public IEnumerable<(string name, IEnumerable<ReusableObjectGraph> value)> ComplexProps;
        public IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleConstructorArgs;
        public IEnumerable<(int argIndex, IEnumerable<ReusableObjectGraph> value)> ComplexConstructorArgs;
        public Type[] CArgTypes;

        public override Type[] ConstructorArgTypes => CArgTypes ?? new Type[0];
        
        public override IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> GetSimpleProps() => SimpleProps ?? Enumerable.Empty<(string, IEnumerable<object>, bool)>();
        
        public override IEnumerable<(string name, IEnumerable<ReusableObjectGraph> value)> GetComplexProps() => ComplexProps ?? Enumerable.Empty<(string, IEnumerable<ReusableObjectGraph>)>();
        
        public override IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> GetSimpleConstructorArgs() => SimpleConstructorArgs ?? Enumerable.Empty<(int, IEnumerable<object>, bool)>();
        
        public override IEnumerable<(int argIndex, IEnumerable<ReusableObjectGraph> value)> GetComplexConstructorArgs() => ComplexConstructorArgs ?? Enumerable.Empty<(int, IEnumerable<ReusableObjectGraph>)>();
    }
}