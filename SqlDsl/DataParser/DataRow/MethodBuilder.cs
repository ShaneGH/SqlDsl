using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
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
        private static readonly ReadOnlyCollection<ITypeUtils> ValueObjectMethods = 
            new ReadOnlyCollection<ITypeUtils>(GetValueObjectReadMethods().ToList());

        public static void ImplementIDataRow(TypeBuilder toType, FieldInfo[] fields)
        {
            var buildGetMethodFields = new List<(int, FieldInfo)>(fields.Length);
            var otherMethodFields = new List<(int, FieldInfo)>(fields.Length);
            buildGetMethodFields.AddRange(fields.Select((x, i) => (i, x)));
            otherMethodFields.AddRange(fields.Select((x, i) => (i, x)));

            foreach (var methods in ValueObjectMethods)
            {
                var typedFields = buildGetMethodFields
                    .Where(f => f.Item2.FieldType == methods.Type)
                    .ToList();

                typedFields.ForEach(x => buildGetMethodFields.Remove(x));
                BuildFieldGetMethod(toType, methods.GetMethod, typedFields);
            }

            var refObjectMethod = ReflectionUtils.GetMethod<IDataRow>(x => x.GetValue(0));
            BuildFieldGetMethod(toType, refObjectMethod, buildGetMethodFields);
            BuildValueIsEqualMethod(toType, otherMethodFields);
            BuildHasValueMethod(toType, otherMethodFields);
        }

        private static void BuildHasValueMethod(TypeBuilder type, List<(int i, FieldInfo field)> fieldGetters)
        {            
            var attributes = MethodAttributes.Public |
                MethodAttributes.HideBySig |
                MethodAttributes.NewSlot |
                MethodAttributes.Virtual |
                MethodAttributes.Final;

            var methodBody = type
                .DefineMethod(nameof(IDataRow.HasValue), attributes, typeof(bool), new Type[] { typeof(int) })
                .GetILGenerator();

            List<(Label, FieldInfo)> caseResults = new List<(Label, FieldInfo)>();
            foreach (var (i, field) in fieldGetters.OrderBy(x => x.i))
            {
                methodBody.Emit(OpCodes.Ldarg_1);
                methodBody.Emit(OpCodes.Ldc_I4, i);
                caseResults.Add((
                    methodBody.BranchTo(OpCodes.Beq),
                    field));
            }

            Label returnLabel = methodBody.DefineLabel();
            foreach (var (startLabel, field) in caseResults)
            {
                methodBody.MarkLabel(startLabel);  
                if (field.FieldType.IsClass)
                {
                    methodBody.Emit(OpCodes.Ldarg_0);
                    methodBody.Emit(OpCodes.Ldfld, field);
                    methodBody.Emit(OpCodes.Ldnull);
                    methodBody.Emit(OpCodes.Cgt_Un);
                }
                else if (field.FieldType.IsConstructedGenericType && 
                    field.FieldType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var hasValue = field.FieldType.GetProperty(nameof(Nullable<int>.HasValue)).GetMethod;

                    methodBody.Emit(OpCodes.Ldarg_0);
                    methodBody.Emit(OpCodes.Ldflda, field);
                    methodBody.Emit(OpCodes.Call, hasValue);
                }
                else
                {
                    throw new InvalidOperationException($"Type {field.FieldType} must be nullable");
                }
                
                methodBody.Emit(OpCodes.Br, returnLabel);
            }

            var throwMethod = ReflectionUtils
                .GetMethod(() => Compiler.ThrowExceptionForDynamicTypeIndex<object>(null, 0), typeof(bool));

            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));

            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Call, throwMethod);
            methodBody.Emit(OpCodes.Br, returnLabel);
            methodBody.MarkLabel(returnLabel);
            methodBody.Emit(OpCodes.Ret);
        }

        private static void BuildValueIsEqualMethod(TypeBuilder type, List<(int i, FieldInfo field)> fieldGetters)
        {            
            var attributes = MethodAttributes.Public |
                MethodAttributes.HideBySig |
                MethodAttributes.NewSlot |
                MethodAttributes.Virtual |
                MethodAttributes.Final;

            var methodBody = type
                .DefineMethod(nameof(IDataRow.ValueIsEqual), attributes, typeof(bool), new Type[] { typeof(IDataRow), typeof(int) })
                .GetILGenerator();
                                
            Label returnLabel = methodBody.DefineLabel();
            Label switchLabel = methodBody.DefineLabel();
            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Isinst, type);
            methodBody.Emit(OpCodes.Ldnull);
            methodBody.Emit(OpCodes.Ceq);
            methodBody.Emit(OpCodes.Brfalse, switchLabel);
            
            methodBody.Emit(OpCodes.Ldc_I4_0);
            methodBody.Emit(OpCodes.Br, returnLabel);

            methodBody.MarkLabel(switchLabel);

            List<(Label, FieldInfo)> caseResults = new List<(Label, FieldInfo)>();
            foreach (var (i, field) in fieldGetters)
            {
                methodBody.Emit(OpCodes.Ldarg_2);
                methodBody.Emit(OpCodes.Ldc_I4, i);
                caseResults.Add((
                    methodBody.BranchTo(OpCodes.Beq),
                    field));
            }

            foreach (var (startLabel, field) in caseResults)
            {
                methodBody.MarkLabel(startLabel);  
                if (field.FieldType.IsClass)
                {
                    methodBody.Emit(OpCodes.Ldarg_0);
                    methodBody.Emit(OpCodes.Ldfld, field);
                    methodBody.Emit(OpCodes.Ldarg_1);
                    methodBody.Emit(OpCodes.Ldfld, field);
                    methodBody.Emit(OpCodes.Ceq);
                }
                else if (field.FieldType.IsConstructedGenericType && 
                    field.FieldType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    methodBody.Emit(OpCodes.Ldarg_0);
                    methodBody.Emit(OpCodes.Ldfld, field);
                    methodBody.Emit(OpCodes.Ldarg_1);
                    methodBody.Emit(OpCodes.Ldfld, field);
                    methodBody.Emit(
                        OpCodes.Call, 
                        ReflectionUtils.GetMethod(() => CompareStructs<int>(1, 1), field.FieldType.GetGenericArguments()[0]));
                }
                else
                {
                    throw new InvalidOperationException($"Type {field.FieldType.IsConstructedGenericType} must be nullable");
                }
                
                methodBody.Emit(OpCodes.Br, returnLabel);
            }

            var throwMethod = ReflectionUtils
                .GetMethod(() => Compiler.ThrowExceptionForDynamicTypeIndex<object>(null, 0), typeof(bool));

            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));

            methodBody.Emit(OpCodes.Ldarg_2);
            methodBody.Emit(OpCodes.Call, throwMethod);
            methodBody.Emit(OpCodes.Br, returnLabel);
            methodBody.MarkLabel(returnLabel);
            methodBody.Emit(OpCodes.Ret);
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

        public static bool CompareStructs<T>(T? val1, T? val2)
            where T: struct => val1.Equals(val2);

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

            List<(Label, FieldInfo)> caseResults = new List<(Label, FieldInfo)>();
            foreach (var (i, field) in fieldGetters
                .OrderBy(x => OrderField(x.field, method)))
            {
                methodBody.Emit(OpCodes.Ldarg_0);
                methodBody.Emit(OpCodes.Ldc_I4, i);
                caseResults.Add((
                    methodBody.BranchTo(OpCodes.Beq),
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

                methodBody.Emit(OpCodes.Br, returnLabel);
            }

            var throwMethod = ReflectionUtils
                .GetMethod(() => Compiler.ThrowExceptionForDynamicTypeIndex<object>(null, 0), method.ReturnType);

            methodBody.Emit(OpCodes.Ldarg_0);
            methodBody.Emit(OpCodes.Call, ReflectionUtils.GetMethod<object>(x => x.GetType()));

            methodBody.Emit(OpCodes.Ldarg_1);
            methodBody.Emit(OpCodes.Call, throwMethod);
            methodBody.Emit(OpCodes.Br, returnLabel);
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

        static IEnumerable<ITypeUtils> GetValueObjectReadMethods()
        {
            yield return Create(x => x.GetBoolean(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetByte(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetChar(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetDateTime(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetDecimal(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetDouble(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetFloat(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetGuid(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetInt16(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetInt32(0), (x, y) => AreEqual(x, y));
            yield return Create(x => x.GetInt64(0), (x, y) => AreEqual(x, y));
        }

        static bool AreEqual(bool? v1, bool? v2) => v1 == v2;
        static bool AreEqual(byte? v1, byte? v2) => v1 == v2;
        static bool AreEqual(char? v1, char? v2) => v1 == v2;
        static bool AreEqual(DateTime? v1, DateTime? v2) => v1 == v2;
        static bool AreEqual(decimal? v1, decimal? v2) => v1 == v2;
        static bool AreEqual(double? v1, double? v2) => v1 == v2;
        static bool AreEqual(float? v1, float? v2) => v1 == v2;
        static bool AreEqual(Guid? v1, Guid? v2) => v1 == v2;
        static bool AreEqual(short? v1, short? v2) => v1 == v2;
        static bool AreEqual(int? v1, int? v2) => v1 == v2;
        static bool AreEqual(long? v1, long? v2) => v1 == v2;

        private static TypeUtils<T> Create<T>(Expression<Func<IDataRow, T?>> getMethod, Expression<Func<T?, T?, bool>> areEqualMethod)
            where T: struct => new TypeUtils<T>(getMethod, areEqualMethod);

        private class TypeUtils<T> : ITypeUtils
            where T: struct
        {
            static readonly Type _type = typeof(T?);
            public MethodInfo GetMethod { get; }
            public MethodInfo AreEqualMethod { get; }

            public Type Type => _type;

            public TypeUtils(Expression<Func<IDataRow, T?>> getMethod, Expression<Func<T?, T?, bool>> areEqualMethod)
            {
                GetMethod = GetMethodInfo(getMethod.Body);
                AreEqualMethod = GetMethodInfo(areEqualMethod.Body);

                MethodInfo GetMethodInfo(Expression e)
                {
                    if (!(e is MethodCallExpression method))
                        throw new InvalidOperationException();

                    return method.Method;
                }
            }
        }
    }
}

/*using System;
using System.Collections.Generic;
using System.Data;

namespace il
{
    public class TypeHolder
    {
        //dotnet build; dotnet ildasm "C:\Dev\Messing\il\bin\Debug\netstandard2.0\il.dll"
        public static readonly Dictionary<Type, Type[]> Types = new Dictionary<Type, Type[]>
        {
        };
        
        public static Type[] GetTypes(Type type)
        {
            throw new InvalidCastException();
        }
        
        public static T Throw<T>(Type type, int x)
        {
            throw new InvalidCastException();
        }
    }

    public class MyC : III
    {
        private readonly int typeKey;
        public int? MyProp1;
        public int? MyProp2;
        public long? MyProp3;
        public object MyProp4;

        public MyC(IDataRecord data, int typeKey)
        {
            MyProp1 = data.GetInt32(111);
            MyProp2 = data.GetInt32(222);
            MyProp3 = data.GetInt32(333);
            this.typeKey = typeKey;
        }

        public int? GetInt(int index)
        {
            switch (index)
            {
                case 111:
                    return MyProp1;
                case 222:
                    return MyProp2;
                case 333:
                    return Convert.ToInt32(MyProp3);
                default:
                    return TypeHolder.Throw<int>(null, index);
            }
        }

        public bool HasValue(int index)
        {
            switch (index)
            {
                case 111:
                    return CompareStructs(MyProp1, default(int?));
                case 222:
                    return CompareStructs(MyProp2, default(int?));
                case 333:
                    return CompareStructs(MyProp3, default(int?));
                case 444:
                    return MyProp4 != null;
                default:
                    return TypeHolder.Throw<bool>(null, index);
            }
        }

        public static bool CompareStructs<T>(T? i1, T? i2) where T: struct => true;

        public bool ValueIsEqual(III otherRow, int index)
        {
            if (!(otherRow is MyC x))
                return false;

            switch (index)
            {
                case 111:
                    return MyProp1 == x.MyProp1;
                case 222:
                    return MyProp2 == x.MyProp2;
                case 333:
                    return MyProp3 == x.MyProp3;
                case 444:
                    return MyProp4 == x.MyProp4;
                default:
                    return TypeHolder.Throw<bool>(null, index);
            }
        }
    }

    public interface III
    {
        int? GetInt(int index);
    }
}
*/