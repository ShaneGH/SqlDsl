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
    /// <summary>
    /// Build a constructor with initial property setters
    /// </summary>
    public class ConstructorBuilder
    {
        private static readonly ConstructorInfo _baseConstructor = typeof(object).GetConstructor(new Type[0]);
        private static readonly ReadOnlyDictionary<Type, MethodInfo> ReadMethods = 
            new ReadOnlyDictionary<Type, MethodInfo>(BuildReadMethods().ToDictionary(x => x.ReturnType));

        public static void Build(TypeBuilder toType, FieldInfo[] fields)
        {
            var methodBody = toType
                .DefineConstructor(MethodAttributes.Public, CallingConventions.HasThis, new Type[] { typeof(IDataRecord) })
                .GetILGenerator();

            var ret = methodBody.DefineLabel();

            // Call base constructor
            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Call, _baseConstructor);

            // set values for each field
            for (var i = 0; i < fields.Length; i++)
            {
                SetValue(methodBody, fields[i], i);
            }

            methodBody.MarkLabel(ret);
            methodBody.Emit(OpCodes.Ret);
        }

        private static void SetValue(ILGenerator methodBody, FieldInfo field, int index)
        {
            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Ldc_I4, index);
            methodBody.Emit(OpCodes.Call, GetParseMethod(field.FieldType));
            methodBody.Emit(OpCodes.Stfld, field);
        }

        private static MethodInfo GetParseMethod(Type type)
        {
            if (!ReadMethods.TryGetValue(type, out var result))
            {
                return ReadMethods[typeof(object)];
            }

            return result;
        }

        static IEnumerable<MethodInfo> BuildReadMethods()
        {
            yield return ReflectionUtils.GetMethod(() => GetBooleanNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetByteNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetCharNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetDateTimeNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetDecimalNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetDoubleNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetFloatNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetGuidNullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetInt16Nullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetInt32Nullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetInt64Nullable(null, 0));
            yield return ReflectionUtils.GetMethod(() => GetValue(null, 0));
        }

        public static bool? GetBooleanNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new bool?() : data.GetBoolean(index);

        public static char? GetCharNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new char?() : data.GetChar(index);

        public static byte? GetByteNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new byte?() : data.GetByte(index);

        public static DateTime? GetDateTimeNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new DateTime?() : data.GetDateTime(index);

        public static decimal? GetDecimalNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new decimal?() : data.GetDecimal(index);

        public static double? GetDoubleNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new double?() : data.GetDouble(index);

        public static float? GetFloatNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new float?() : data.GetFloat(index);

        public static Guid? GetGuidNullable(IDataRecord data, int index) => data.IsDBNull(index) ? new Guid?() : data.GetGuid(index);

        public static Int16? GetInt16Nullable(IDataRecord data, int index) => data.IsDBNull(index) ? new short?() : data.GetInt16(index);

        public static int? GetInt32Nullable(IDataRecord data, int index) => data.IsDBNull(index) ? new int?() : data.GetInt32(index);

        public static long? GetInt64Nullable(IDataRecord data, int index) => data.IsDBNull(index) ? new long?() : data.GetInt64(index);

        public static object GetValue(IDataRecord data, int index) => data.GetValue(index);
    }
}