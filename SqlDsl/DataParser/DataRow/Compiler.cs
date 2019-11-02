using System;
using System.Collections.Generic;
using System.Reflection;
using SqlDsl.Utils.EqualityComparers;
using System.Threading.Tasks;
using System.Reflection.Emit;

namespace SqlDsl.DataParser.DataRow
{
    public class Compiler
    {
        readonly Dictionary<Type[], Task> Compiling = new Dictionary<Type[], Task>(ArrayComparer<Type>.Instance);
        private readonly Dictionary<Type[], Type> Built = new Dictionary<Type[], Type>(ArrayComparer<Type>.Instance);

        /// <summary>The opposite of built</summary>
        private readonly Dictionary<Type, Type[]> Reference = new Dictionary<Type, Type[]>();
        private const string RootNamespace = "SqlDsl.DataParser.DynamicObjects";
        static readonly Compiler Instance = new Compiler();

        readonly AssemblyBuilder Assembly;
        readonly ModuleBuilder Module;

        public static Task<Type> Compile(Type[] rowTypes)
        {
            return Instance._Compile(rowTypes);
        }

        private Compiler()
        {
            Assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName(RootNamespace), AssemblyBuilderAccess.Run);
            Module = Assembly.DefineDynamicModule(RootNamespace);
        }

        public static T ThrowExceptionForDynamicTypeIndex<T>(Type type, int typeIndex) => Instance._ThrowExceptionForDynamicTypeIndex<T>(type, typeIndex);

        private T _ThrowExceptionForDynamicTypeIndex<T>(Type type, int typeIndex)
        {
            if (!Reference.TryGetValue(type, out var types))
            {
                throw new InvalidOperationException($"Invalid dynamic type {type}");
            }

            if (types.Length <= typeIndex)
            {
                throw new IndexOutOfRangeException($"{typeIndex} is out of range");
            }

            throw new InvalidOperationException($"Value at column index {typeIndex} is a {types[typeIndex]}");
        }

        async Task<Type> _Compile(Type[] rowTypes)
        {
            if (!Built.ContainsKey(rowTypes))
            {
                await CompileAndCache(rowTypes);
            }

            return Built[rowTypes];
        }

        Task CompileAndCache(Type[] rowTypes)
        {
            Task compiling = null;
            lock (Compiling)
            {
                if (Built.ContainsKey(rowTypes)) return Task.CompletedTask;

                if (Compiling.ContainsKey(rowTypes))
                {
                    compiling = Compiling[rowTypes];
                }
                else
                {
                    Compiling.Add(rowTypes, compiling = Task.Run((Action)Compile));
                }
            }
            
            return compiling;

            void Compile()
            {
                // try
                // {
                    var built = BuildType(rowTypes);
                    lock (Compiling)
                    {
                        Built.Add(rowTypes, built);
                        Reference.Add(built, rowTypes);
                        Compiling.Remove(rowTypes);
                    }
                // }
                // catch (Exception e)
                // {
                //     throw new CompilerException(rowTypes, e);
                // }
            }
        }

        static int TypeIncrement = 0;
        Type BuildType(Type[] rowTypes)
        {
            string className;
            lock (Compiling)
            {
                className = $"{RootNamespace}.DynamicType{++TypeIncrement}";
            }

            // define type
            var type = Module.DefineType(
                className,
                TypeAttributes.Public | TypeAttributes.Class,
                typeof(object),
                new Type[] { typeof(IDataRow) });

            var fields = new FieldInfo[rowTypes.Length];
            for (var i = 0; i < rowTypes.Length; i++)
            {
                var fieldType = rowTypes[i].IsClass
                    ? rowTypes[i]
                    : typeof(Nullable<>).MakeGenericType(rowTypes[i]);

                fields[i] = type.DefineField(
                    $"Cell{ToString(i)}", 
                    fieldType,
                    FieldAttributes.NotSerialized | FieldAttributes.Public | FieldAttributes.InitOnly);
            }

            ConstructorBuilder.Build(type, fields);
            IDataRowImplementer.ImplementIDataRow(type, fields);

            return type.CreateTypeInfo();
        }

        static string ToString(int i)
        {
            
            if (i < 10) return $"0{i}";
            return i.ToString();
        }
    }
}