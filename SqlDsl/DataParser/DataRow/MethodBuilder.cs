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
    public static class IDataRowImplementer
    {
        private static readonly ReadOnlyCollection<MethodInfo> ValueObjectReadMethods = 
            new ReadOnlyCollection<MethodInfo>(GetValueObjectReadMethods().ToList());

        public static void ImplementIDataRow(TypeBuilder toType, FieldInfo[] fields)
        {
            var flds = new List<(int, FieldInfo)>(fields.Length);
            flds.AddRange(fields.Select((x, i) => (i, x)));

            foreach (var readMethod in ValueObjectReadMethods)
            {
                var typedFields = flds
                    .Where(f => f.Item2.FieldType == readMethod.ReturnType)
                    .ToList();

                typedFields.ForEach(x => flds.Remove(x));
                BuildFieldGetMethod(toType, readMethod, typedFields);
            }

            var refObjectMethod = ReflectionUtils.GetMethod<IDataRow>(x => x.GetValue(0));
            BuildFieldGetMethod(toType, refObjectMethod, flds);
        }

        /*
            switch (index)
            {
                case 1:
                    return MyProp1;
                case 2:
                    return MyProp2;
                case 7:
                    return MyProp7;
                default:
                    return Compiler.ThrowExceptionForDynamicTypeIndex<T>(GetType(), index);
            }
        */

        private static Label BranchTo(this ILGenerator generator, OpCode opCode)
        {
            var label = generator.DefineLabel();
            generator.Emit(opCode, label);

            return label;
        }

        private static HashSet<Type> Numerics = new HashSet<Type>
        {
            typeof(Byte),
            typeof(SByte),
            typeof(Decimal),
            typeof(Double),
            typeof(float),
            typeof(Int16),
            typeof(UInt16),
            typeof(Int32),
            typeof(UInt32),
            typeof(Int64),
            typeof(UInt64)
        };

        static int OrderField(FieldInfo field, MethodInfo forMethod)
        {
            if (field.FieldType == forMethod.ReturnType)
                return 0;
                
            if (Numerics.Contains(forMethod.ReturnType) &&
                Numerics.Contains(field.FieldType))
            {
                return 1;
            }
                
            if (forMethod.ReturnType.IsValueType &&
                field.FieldType.IsValueType &&
                forMethod.ReturnType != typeof(Guid) &&
                field.FieldType != typeof(Guid) &&
                forMethod.ReturnType != typeof(DateTime) &&
                field.FieldType != typeof(DateTime))
            {
                return 2;
            }

            return 3;
        }

        static void BuildFieldGetMethod(TypeBuilder type, MethodInfo method, List<(int i, FieldInfo field)> fieldGetters)
        {
            var attributes = MethodAttributes.Public |
                MethodAttributes.HideBySig |
                MethodAttributes.NewSlot |
                MethodAttributes.Virtual |
                MethodAttributes.Final;

            var methodBody = type
                .DefineMethod(method.Name, attributes, method.ReturnType, new Type[] { typeof(int) })
                .GetILGenerator();

            bool first = true;
            List<(Label, FieldInfo)> caseResults = new List<(Label, FieldInfo)>();
            foreach (var (i, field) in fieldGetters
                .OrderBy(x => OrderField(x.field, method)))
            {
                methodBody.Emit(OpCodes.Ldarg_0);
                if (first)
                    methodBody.Emit(OpCodes.Ldc_I4_S, i);
                else
                    methodBody.Emit(OpCodes.Ldc_I4, i);

                first = false;

                caseResults.Add((
                    methodBody.BranchTo(OpCodes.Beq_S),
                    field));
            }

            Label returnLabel = methodBody.DefineLabel();
            foreach (var (startLabel, field) in caseResults)
            {
                MethodInfo convert = null;
                if (field.FieldType != method.ReturnType)
                {
                    convert = GetConvertMethod(field.FieldType, method.ReturnType);
                }

                methodBody.MarkLabel(startLabel);
                methodBody.Emit(OpCodes.Ldarg_0);
                methodBody.Emit(OpCodes.Ldfld, field);

                // reference types are stored as objects anyway
                if (field.FieldType != method.ReturnType && !field.FieldType.IsClass && !field.FieldType.IsClass)
                {
                    var convertMethod = GetConvertMethod(field.FieldType, method.ReturnType);
                    if (convertMethod != null)
                        methodBody.Emit(OpCodes.Call, convertMethod);
                    else
                        throw new InvalidOperationException($"Cannot convert from type {field.FieldType} to type {method.ReturnType}.");
                }

                methodBody.Emit(OpCodes.Br_S, returnLabel);
            }

            var throwMethod = ReflectionUtils
                .GetMethod(() => Compiler.ThrowExceptionForDynamicTypeIndex<object>(null, 0), method.ReturnType);

            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));

            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Call, throwMethod);
            methodBody.Emit(OpCodes.Br_S, returnLabel);
            methodBody.MarkLabel(returnLabel);
            methodBody.Emit(OpCodes.Ret);
        }

        static MethodInfo GetConvertMethod(Type from, Type to)
        {
            return typeof(Convert)
                .GetMethods()
                .Where(m => m.IsStatic && m.IsPublic)
                .Where(m => m.ReturnType == to)
                .Where(m => m.Name == "To" + to.Name)
                .Where(m => m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == from)
                .FirstOrDefault();
        }

        static IEnumerable<MethodInfo> GetValueObjectReadMethods()
        {
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetBoolean(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetByte(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetChar(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetDateTime(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetDecimal(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetDouble(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetFloat(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetGuid(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetInt16(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetInt32(0));
            yield return ReflectionUtils.GetMethod<IDataRow>(x => x.GetInt64(0));
        }
    }
}