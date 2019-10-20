using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using SqlDsl.Utils;

namespace SqlDsl.DataParser.DataRow
{
    public interface IDataRow
    {
        bool GetBoolean(int i);
        byte GetByte(int i);
        char GetChar(int i);
        DateTime GetDateTime(int i);
        Decimal GetDecimal(int i);
        Double GetDouble(int i);
        float GetFloat(int i);
        Guid GetGuid(int i);
        short GetInt16(int i);
        int GetInt32(int i);
        long GetInt64(int i);
        object GetValue(int i);
    }
}