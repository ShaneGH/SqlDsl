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
            methodBody.Emit(OpCodes.Callvirt, GetParseMethod(field.FieldType));
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
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetBoolean(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetByte(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetChar(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetDateTime(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetDecimal(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetDouble(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetFloat(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetGuid(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetInt16(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetInt32(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetInt64(0));
            yield return ReflectionUtils.GetMethod<IDataRecord>(x => x.GetValue(0));
        }
    }
}