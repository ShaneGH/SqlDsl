using System;

namespace SqlDsl.DataParser
{
    public class SimpleConstructorArg
    {
        public readonly int Index;
        public readonly int ArgIndex;
        public readonly int[] PrimaryKeyColumns;
        public readonly Type ResultPropertyType;
        public readonly Type DataCellType;

        public SimpleConstructorArg(int index, int argIndex, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)
        {
            this.Index = index;
            this.ArgIndex = argIndex;
            this.PrimaryKeyColumns = primaryKeyColumns;
            this.ResultPropertyType = resultPropertyType;
            this.DataCellType = dataCellType;
        }
    }
}