using SqlDsl.DataParser.DataRow;

namespace SqlDsl.ObjectBuilders
{
    public struct DataRowValue
    {
        public readonly IDataRow DataRow;
        public readonly short Index;

        public DataRowValue(IDataRow dataRow, short index)
        {
            this.DataRow = dataRow;
            this.Index = index;
        }

        public object ToObj()
        {
            return DataRow.ToObj()[Index];
        }
    }
}
