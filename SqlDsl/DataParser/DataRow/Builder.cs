using System;
using System.Collections.Generic;
using System.Reflection;
using SqlDsl.Utils.EqualityComparers;
using System.Threading.Tasks;
using System.Reflection.Emit;
using System.Data;
using System.Linq.Expressions;

namespace SqlDsl.DataParser.DataRow
{
    public class Builder
    {
        static readonly Dictionary<Type[], Func<IDataReader, IDataRow>> Constructors = new Dictionary<Type[], Func<IDataReader, IDataRow>>(ArrayComparer<Type>.Instance);
        static readonly Dictionary<Type[], Task> Compiling = new Dictionary<Type[], Task>(ArrayComparer<Type>.Instance);

        public static async Task<Func<IDataReader, IDataRow>> Build(Type[] forTypes)
        {
            // TODO: value task
            if (Constructors.TryGetValue(forTypes, out Func<IDataReader, IDataRow> constructor))
            {
                return constructor;
            }
            
            Task compiling = null;
            lock (Compiling)
            {
                if (Constructors.TryGetValue(forTypes, out constructor))
                {
                    return constructor;
                }

                if (!Compiling.TryGetValue(forTypes, out compiling))
                {
                    compiling = CompileAndCache(forTypes);
                    Compiling.Add(forTypes, compiling);
                }
            }

            await compiling;
            lock (Compiling)
            {
                Compiling.Remove(forTypes);
            }

            return Constructors[forTypes];
        }

        private static async Task CompileAndCache(Type[] forTypes)
        {
            var type = await Compiler.Compile(forTypes);

            var arg = Expression.Parameter(typeof(IDataReader));
            var constructor = Expression
                .Lambda<Func<IDataReader, IDataRow>>(Expression
                    .New(type.GetConstructor(new [] { typeof(IDataReader) }), arg),
                    arg)
                .Compile();

            Constructors.Add(forTypes, constructor);
        }
    }
}