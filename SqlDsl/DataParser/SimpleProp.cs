using System;

namespace SqlDsl.DataParser
{
    public class SimpleProp
    {
        public readonly int Index;
        public readonly string Name;
        public readonly int[] PrimaryKeyColumns;
        public readonly Type ResultPropertyType;
        public readonly Type DataCellType;

        public SimpleProp(int index, string name, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType)
        {
            this.Index = index;
            this.Name = name;
            this.PrimaryKeyColumns = primaryKeyColumns;
            this.ResultPropertyType = resultPropertyType;
            this.DataCellType = dataCellType;
        }
    }
}