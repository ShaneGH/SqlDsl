using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using Ex = System.Linq.Expressions.Expression;

namespace SqlDsl.DataParser.DPP
{
    interface IPropertyPopulator
    {
        [PropertyCache(nameof(IDataRecord.GetBoolean))]
        List<(string name, bool value)> BooleanProps { get; set; }

        [PropertyCache(nameof(IDataRecord.GetByte))]
        List<(string name, byte value)> ByteProps { get; set; }

        [PropertyCache(nameof(IDataRecord.GetChar))]
        List<(string name, char value)> CharProps { get; set; }

        [PropertyCache(nameof(IDataRecord.GetDateTime))]
        List<(string name, DateTime value)> DateTimeProps { get; set; }

        [PropertyCache(nameof(IDataRecord.GetDecimal))]
        List<(string name, decimal value)> DecimalProps { get; set; }

        [PropertyCache(nameof(IDataRecord.GetDouble))]
        List<(string name, double value)> DoubleProps { get; set; }

        [PropertyCache(nameof(IDataRecord.GetFloat))]
        List<(string name, float value)> FloatProps { get; set; }
        
        [PropertyCache(nameof(IDataRecord.GetGuid))]
        List<(string name, Guid value)> GuidProps { get; set; }
        
        [PropertyCache(nameof(IDataRecord.GetInt16))]
        List<(string name, short value)> Int16Props { get; set; }
        
        [PropertyCache(nameof(IDataRecord.GetInt32))]
        List<(string name, int value)> Int32Props { get; set; }
        
        [PropertyCache(nameof(IDataRecord.GetInt64))]
        List<(string name, long value)> Int64Props { get; set; }
        
        // todo: use this as a fallback for types which
        // cannot be parsed by other means
        [PropertyCache(nameof(IDataRecord.GetValue))]
        List<(string name, object value)> ReferenceObjectProps { get; set; }
    }

    [System.AttributeUsage(System.AttributeTargets.All, Inherited = false, AllowMultiple = false)]
    class PropertyCacheAttribute : System.Attribute
    {
        public readonly string DataReaderMethod;

        public PropertyCacheAttribute(string dataReaderMethod)
        {
            DataReaderMethod = dataReaderMethod;
        }
    }
}