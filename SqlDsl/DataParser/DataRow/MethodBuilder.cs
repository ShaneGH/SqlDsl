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
            
        static MethodAttributes GetAttrs()
        {
            return MethodAttributes.HideBySig | MethodAttributes.Virtual | MethodAttributes.Public;
        }

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

            methodBody.Emit(OpCodes.Nop);
            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Stloc_0);

            bool first = true;
            List<(Label, FieldInfo)> caseResults = new List<(Label, FieldInfo)>();
            foreach (var (i, field) in fieldGetters)
            {
                methodBody.Emit(OpCodes.Ldloc_0);
                if (first)
                    methodBody.Emit(OpCodes.Ldc_I4_S, i);
                else
                    methodBody.Emit(OpCodes.Ldc_I4, i);

                first = false;

                caseResults.Add((
                    methodBody.BranchTo(OpCodes.Beq_S),
                    field));
                var nextCaseStart = methodBody.BranchTo(OpCodes.Br_S);
                methodBody.MarkLabel(nextCaseStart);
            }

            Label returnLabel = methodBody.DefineLabel();
            foreach (var (startLabel, field) in caseResults)
            {
                methodBody.MarkLabel(startLabel);
                methodBody.Emit(OpCodes.Ldarg_0);
                methodBody.Emit(OpCodes.Ldfld, field);
                methodBody.Emit(OpCodes.Stloc_1);
                methodBody.Emit(OpCodes.Br_S, returnLabel);
            }

            var throwMethod = ReflectionUtils
                .GetMethod(() => Compiler.ThrowExceptionForDynamicTypeIndex<object>(null, 0), method.ReturnType);

            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));
            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Call, throwMethod);
            methodBody.Emit(OpCodes.Stloc_1);
            methodBody.Emit(OpCodes.Br_S, returnLabel);
            methodBody.MarkLabel(returnLabel);
            methodBody.Emit(OpCodes.Ldloc_1);
            methodBody.Emit(OpCodes.Ret);
            
    // IL_0039: ldarg.0
    // IL_003a: call instance [netstandard]System.Type [netstandard]System.Object::GetType()
    // IL_003f: ldarg.1
    // IL_0040: call mvar il.TypeHolder::Throw([netstandard]System.Type, int32)
    // IL_0045: stloc.1
    // IL_0046: br.s     IL_0048
    // IL_0048: ldloc.1
    // IL_0049: ret


            // // // // methodBody.Emit(OpCodes.Ldloc_0);
            // // // // methodBody.Emit(OpCodes.Ldc_I4_S, 111);
            // // // // var IL_001e = methodBody.BranchTo(OpCodes.Beq_S);
            // // // // var IL_000a = methodBody.BranchTo(OpCodes.Br_S);
            
            // // // // methodBody.MarkLabel(IL_000a);
            // // // // methodBody.Emit(OpCodes.Ldloc_0);
            // // // // methodBody.Emit(OpCodes.Ldc_I4, 222);
            // // // // var IL_0027 = methodBody.BranchTo(OpCodes.Beq_S);
            // // // // var IL_0014 = methodBody.BranchTo(OpCodes.Br_S);
            
            // // // // methodBody.MarkLabel(IL_0014);
            // // // // methodBody.Emit(OpCodes.Ldloc_0);
            // // // // methodBody.Emit(OpCodes.Ldc_I4, 333);
            // // // // var IL_0030 = methodBody.BranchTo(OpCodes.Beq_S);
            // // // // var IL_0039 = methodBody.BranchTo(OpCodes.Br_S);
            
            // // // // methodBody.MarkLabel(IL_001e);
            // // // // methodBody.Emit(OpCodes.Ldarg_0);
            // // // // methodBody.Emit(OpCodes.Ldfld, F111);
            // // // // methodBody.Emit(OpCodes.Stloc_1);
            // // // // var IL_0094 = methodBody.BranchTo(OpCodes.Br_S);

            // // // // methodBody.MarkLabel(IL_0027);
            // // // // methodBody.Emit(OpCodes.Ldarg_0);
            // // // // methodBody.Emit(OpCodes.Ldfld, F222);
            // // // // methodBody.Emit(OpCodes.Stloc_1);
            // // // // methodBody.Emit(OpCodes.Br_S, IL_0094);
            
            // // // // methodBody.MarkLabel(IL_0030);
            // // // // methodBody.Emit(OpCodes.Ldarg_0);
            // // // // methodBody.Emit(OpCodes.Ldfld, F333);
            // // // // methodBody.Emit(OpCodes.Stloc_1);
            // // // // methodBody.Emit(OpCodes.Br_S, IL_0094);
            
            // if (nextCaseStart != null)
            //     methodBody.MarkLabel(nextCaseStart.Value);

            // methodBody.Emit(OpCodes.Ldarg_0);
            // methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));
            // methodBody.Emit(OpCodes.Call, GetTypesMethod);

            // methodBody.Emit(OpCodes.Ldlen);
            // methodBody.Emit(OpCodes.Conv_I4);
            // methodBody.Emit(OpCodes.Ldarg_1);
            // methodBody.Emit(OpCodes.Cgt);
            // methodBody.Emit(OpCodes.Ldc_I4_0);
            // methodBody.Emit(OpCodes.Ceq);
            // methodBody.Emit(OpCodes.Stloc_2);
            // methodBody.Emit(OpCodes.Ldloc_2);
            // var IL_006c = methodBody.BranchTo(OpCodes.Brfalse_S);
            // methodBody.Emit(OpCodes.Nop);
            // methodBody.Emit(OpCodes.Ldstr, "{0} is out of range.");
            // methodBody.Emit(OpCodes.Ldarg_1);
            // methodBody.Emit(OpCodes.Box, typeof(int));
            // methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod(() => string.Format("", 4)));
            // methodBody.Emit(OpCodes.Newobj, typeof(IndexOutOfRangeException).GetConstructor(new[]{typeof(string)}));
            // methodBody.Emit(OpCodes.Throw);
            // methodBody.MarkLabel(IL_006c);
            // methodBody.Emit(OpCodes.Ldstr, "Value {0} is a {1}.");
            // methodBody.Emit(OpCodes.Ldarg_1);
            // methodBody.Emit(OpCodes.Box, typeof(int));
            
            // methodBody.Emit(OpCodes.Ldarg_0);
            // methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));
            // methodBody.Emit(OpCodes.Call, GetTypesMethod);
            // methodBody.Emit(OpCodes.Ldarg_1);
            // methodBody.Emit(OpCodes.Ldelem_Ref);
            // methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod(() => string.Format("", 4, 4)));
            // methodBody.Emit(OpCodes.Newobj, typeof(InvalidOperationException).GetConstructor(new[]{typeof(string)}));
            // methodBody.Emit(OpCodes.Throw);
            
            // if (retrunLabel != null)
            //     methodBody.MarkLabel(retrunLabel.Value);

            // methodBody.Emit(OpCodes.Ldloc_1);
            // methodBody.Emit(OpCodes.Ret);
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