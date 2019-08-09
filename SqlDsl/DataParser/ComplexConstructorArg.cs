using System;

namespace SqlDsl.DataParser
{
    public class ComplexConstructorArg
    {
        public readonly int ArgIndex;
        public readonly Type ConstuctorArgType;
        public readonly ObjectPropertyGraph Value;

        public ComplexConstructorArg(int argIndex, Type constuctorArgType, ObjectPropertyGraph value)
        {
            this.ArgIndex = argIndex;
            this.ConstuctorArgType = constuctorArgType;
            this.Value = value;
        }
    }
}