namespace SqlDsl.DataParser
{
    public class ComplexProp
    {
        public readonly string Name;
        public readonly ObjectPropertyGraph Value;

        public ComplexProp(string name, ObjectPropertyGraph value)
        {
            this.Name = name;
            this.Value = value;
        }
    }
}