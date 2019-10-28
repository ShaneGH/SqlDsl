using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace SqlDsl.DataParser.DataRow
{
    public interface IDataRow
    {
        bool? GetBoolean(int i);
        byte? GetByte(int i);
        char? GetChar(int i);
        DateTime? GetDateTime(int i);
        Decimal? GetDecimal(int i);
        Double? GetDouble(int i);
        float? GetFloat(int i);
        Guid? GetGuid(int i);
        short? GetInt16(int i);
        int? GetInt32(int i);
        long? GetInt64(int i);
        object GetValue(int i);
        bool HasValue(int i);
        bool ValueIsEqual(IDataRow otherRow, int valueIndex);
    }

    public static class IDataRowUtils
    {
        public static object[] ToObj(this IDataRow row)
        {
            var cellRx = new Regex(@"^Cell(\d+)$");
            return row
                .GetType()
                .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(x => cellRx.IsMatch(x.Name))
                .OrderBy(x => int.Parse(cellRx.Match(x.Name).Groups[1].Captures[0].Value))
                .Select(x => x.GetValue(row))
                .Select(ConvertFromNullable)
                .ToArray();
        }

        public static object ConvertFromNullable(object o)
        {
            if (o == null) return null;

            var t = o.GetType();
            if (t.IsGenericTypeDefinition
                && t.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return t.GetProperty("Value").GetMethod.Invoke(o, Array.Empty<object>());
            }

            return o;
        }
    }
}