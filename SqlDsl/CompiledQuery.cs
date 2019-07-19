using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl
{
    public class CompiledQuery<TArgs, TResult> : ICompiledQuery<TArgs, TResult>
    {
        internal readonly QueryParts SqlParts;
        readonly object[] Parameters;
        readonly string[] SelectColumns;
        readonly ISqlSyntax SqlFragmentBuilder;
        public readonly RootObjectPropertyGraph PropertyGraph;
        public readonly bool RequiresSimpleValueUnwrap;

        public CompiledQuery(
            QueryParts sql, 
            object[] parameters, 
            string[] selectColumns,
            RootObjectPropertyGraph propertyGraph,
            ISqlSyntax sqlFragmentBuilder,
            bool requiresSimpleValueUnwrap)
        {
            SqlParts = sql;
            Parameters = RewriteParameters(sql.Assemble().sql, parameters);
            SelectColumns = selectColumns;
            PropertyGraph = propertyGraph;
            SqlFragmentBuilder = sqlFragmentBuilder;
            RequiresSimpleValueUnwrap = requiresSimpleValueUnwrap;
        }

        static IEnumerable<(string name, object value)> GetParamValue(object param, TArgs args, int i)
        {
            var name = $"@p{i}";
            if (param is IQueryArgAccessor<TArgs>)
            {
                yield return (name, (param as IQueryArgAccessor<TArgs>).GetArgValue(args));
                yield break;
            }

            if (param is MultiQueryArgAccessor)
            {
                var i2 = -1;
                foreach (var item in (param as MultiQueryArgAccessor).GetMultiArgValue(args))
                    yield return ($"{name}_{++i2}", item);

                yield break;
            }
            
            yield return (name, param);
        }

        IEnumerable<(string name, object value)> BuildParameters(TArgs args)
        {
            return Parameters
                .SelectMany((p, i) => GetParamValue(p, args, i))
                .Select(p => (p.name, p.value ?? DBNull.Value));
        }

        async Task<IEnumerable<object[]>> LoadDataAsync(IExecutor executor, TArgs args, ILogger logger)
        {                    
            var (sql, teardown) = BuildSql(args);
            if (logger.CanLogInfo(LogMessages.ExecutingQuery))
                logger.LogInfo($"Executing sql:{Environment.NewLine}{sql}", LogMessages.ExecutingQuery);

            var timer = new Timer(true);

            // execute and get all rows
            IEnumerable<object[]> results;
            using (var reader = await executor
                .ExecuteDebugAsync(sql, BuildParameters(args), SelectColumns)
                .ConfigureAwait(false))
            {
                results = await reader
                    .GetRowsAsync()
                    .ConfigureAwait(false);

                results = results.Enumerate();
            }

            // TODO: try catch to give any exceptions some context
            if (!string.IsNullOrWhiteSpace(teardown))
                await executor
                    .ExecuteCommandAsync(teardown, CodingConstants.Empty.StringObject)
                    .ConfigureAwait(false);

            if (logger.CanLogInfo(LogMessages.ExecutedQuery))
                logger.LogInfo($"Executed sql in {timer.SplitString()}", LogMessages.ExecutedQuery);

            return results;
        }

        IEnumerable<object[]> LoadData(IExecutor executor, TArgs args, ILogger logger)
        {
            var (sql, teardown) = BuildSql(args);
            if (logger.CanLogInfo(LogMessages.ExecutingQuery))
                logger.LogInfo($"Executing sql:{Environment.NewLine}{sql}", LogMessages.ExecutingQuery);

            var timer = new Timer(true);
            
            // execute and get all rows
            IEnumerable<object[]> results;
            using (var reader = executor.ExecuteDebug(sql, BuildParameters(args), SelectColumns))
                results = reader.GetRows().Enumerate();

            if (!string.IsNullOrWhiteSpace(teardown))
                executor.ExecuteCommand(teardown, CodingConstants.Empty.StringObject);
            
            if (logger.CanLogInfo(LogMessages.ExecutedQuery))
                logger.LogInfo($"Executed sql in {timer.SplitString()}", LogMessages.ExecutedQuery);

            return results;
        }

        static readonly Regex InParamRegex = new Regex(
            @"@p(\d+)" +
            $"{Regex.Escape(SqlStatementConstants.ParamArrayFlag)}", 
            RegexOptions.Compiled);

        static IEnumerable<int> GetInParameters(string whereSql, object[] parameters)
        {
            var inParam = InParamRegex.Match(whereSql);
            while (inParam.Success)
            {
                if (!int.TryParse(inParam.Groups[1].Value, out int paramIndex))
                    throw new InvalidOperationException($"Invalid IN param in {whereSql}");

                if (parameters.Length <= paramIndex)
                    throw new InvalidOperationException($"Param @p{paramIndex} must be enumerable");

                yield return paramIndex;

                inParam = inParam.NextMatch();
            }
        }

        static object[] RewriteParameters(string whereSql, object[] parameters)
        {
            var output = new object[parameters.Length];
            Array.Copy(parameters, output, output.Length);

            foreach (var en in GetInParameters(whereSql, output))
            {
                if (output.Length <= en)
                    throw new InvalidOperationException($"Param @p{en} must be enumerable");

                output[en] = new MultiQueryArgAccessor(output[en]);
            }

            return output;
        }

        public (string sql, string teardownSql) BuildSql(TArgs args)
        {
            return SqlParts.Assemble(x => RewriteSql(args, x));
        }

        string RewriteSql(TArgs args, string sql)
        {
            Match inParam;
            while ((inParam = InParamRegex.Match(sql)).Success)
            {
                if (!int.TryParse(inParam.Groups[1].Value, out int paramIndex))
                    throw new InvalidOperationException($"Invalid IN param in {sql}");

                if (Parameters.Length <= paramIndex)
                    throw new InvalidOperationException($"Param @p{paramIndex} must be enumerable");

                var newParamNames = GetParamValue(Parameters[paramIndex], args, paramIndex)
                    .Select(x => x.name)
                    .ToArray();

                var newParams = newParamNames.Length == 0 ?
                    "" : 
                    newParamNames.Aggregate(SqlFragmentBuilder.BuildCommaCondition);

                sql = sql.Substring(0, inParam.Index) + 
                    newParams + 
                    sql.Substring(inParam.Length + inParam.Index);
            }

            return sql;
        }

        /// <inheritdoc />
        public IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null) 
        {
            return LoadData(executor, args, logger)
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var results = await LoadDataAsync(executor, args, logger).ConfigureAwait(false);
            return results
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap);
        }

        /// <inheritdoc />
        public List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = ToIEnumerable(executor, args, logger);
            var result = enumerable.ToList();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);
            
            return result;
        }

        /// <inheritdoc />
        public async Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = await ToIEnumerableAsync(executor, args, logger).ConfigureAwait(false);
            var result = enumerable.ToList();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = ToIEnumerable(executor, args, logger);
            var result = enumerable.ToArray();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }

        /// <inheritdoc />
        public async Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = await ToIEnumerableAsync(executor, args, logger).ConfigureAwait(false);
            var result = enumerable.ToArray();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public TResult First(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = LoadData(executor, args, logger)
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .First();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public async Task<TResult> FirstAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = (await LoadDataAsync(executor, args, logger)
                .ConfigureAwait(false))
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .First();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public TResult FirstOrDefault(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = LoadData(executor, args, logger)
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .FirstOrDefault();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public async Task<TResult> FirstOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = (await LoadDataAsync(executor, args, logger)
                .ConfigureAwait(false))
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .FirstOrDefault();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public TResult Single(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = LoadData(executor, args, logger)
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .Single();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public async Task<TResult> SingleAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = (await LoadDataAsync(executor, args, logger)
                .ConfigureAwait(false))
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .Single();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public TResult SingleOrDefault(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = LoadData(executor, args, logger)
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .SingleOrDefault();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }
        
        /// <inheritdoc />
        public async Task<TResult> SingleOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = (await LoadDataAsync(executor, args, logger)
                .ConfigureAwait(false))
                .Parse<TResult>(PropertyGraph, logger, RequiresSimpleValueUnwrap)
                .SingleOrDefault();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Data parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }

        private class MultiQueryArgAccessor
        {
            public readonly object OriginalVal;
            
            public MultiQueryArgAccessor(object originalVal)
            {
                OriginalVal = originalVal;
            }

            public IEnumerable<object> GetMultiArgValue(TArgs args)
            {
                var values = GetParamValue(OriginalVal, args, 0).Select(x => x.value);
                return ConvertFromEnumerable(values);
            }

            public IEnumerable<object> ConvertFromEnumerable(IEnumerable<object> enumerables)
            {
                foreach (var ens in enumerables)
                {
                    if (!(ens is IEnumerable))
                        throw new InvalidOperationException("Parameters for a MultiQueryArgAccessor must be enumerable");
                    
                    foreach (var en in ens as IEnumerable)
                        yield return en;
                }
            }
        }
    }

    public class CompiledQuery<TResult> : ICompiledQuery<TResult>
    {
        /// <summary>
        /// Debug and test only. Do not use this property in an application.
        /// </summary>
        internal RootObjectPropertyGraph PropertyGraph => (Worker as CompiledQuery<object, TResult>)?.PropertyGraph;
        
        /// <summary>
        /// Debug only and test only. Do not use this property in an application.
        /// </summary>
        internal string Sql => (Worker as CompiledQuery<object, TResult>)?.BuildSql(null).sql;

        readonly ICompiledQuery<object, TResult> Worker;

        public CompiledQuery(ICompiledQuery<object, TResult> worker)
        {
            Worker = worker ?? throw new ArgumentNullException(nameof(worker));
        }

        /// <inheritdoc />
        public Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, ILogger logger = null) => Worker.ToIEnumerableAsync(executor, null, logger: logger);

        /// <inheritdoc />
        public IEnumerable<TResult> ToIEnumerable(IExecutor executor, ILogger logger = null) => Worker.ToIEnumerable(executor, null, logger: logger);

        /// <inheritdoc />
        public async Task<List<TResult>> ToListAsync(IExecutor executor, ILogger logger = null) => (await ToIEnumerableAsync(executor, logger).ConfigureAwait(false)).ToList();

        /// <inheritdoc />
        public List<TResult> ToList(IExecutor executor, ILogger logger = null) => ToIEnumerable(executor, logger).ToList();

        /// <inheritdoc />
        public async Task<TResult[]> ToArrayAsync(IExecutor executor, ILogger logger = null) => (await ToIEnumerableAsync(executor, logger).ConfigureAwait(false)).ToArray();

        /// <inheritdoc />
        public TResult[] ToArray(IExecutor executor, ILogger logger = null) => ToIEnumerable(executor, logger).ToArray();

        /// <inheritdoc />
        public Task<TResult> FirstAsync(IExecutor executor, ILogger logger = null) => Worker.FirstAsync(executor, null, logger);

        /// <inheritdoc />
        public TResult First(IExecutor executor, ILogger logger = null) => Worker.First(executor, null, logger);

        /// <inheritdoc />
        public Task<TResult> FirstOrDefaultAsync(IExecutor executor, ILogger logger = null) => Worker.FirstOrDefaultAsync(executor, null, logger);

        /// <inheritdoc />
        public TResult FirstOrDefault(IExecutor executor, ILogger logger = null) => Worker.FirstOrDefault(executor, null, logger);

        /// <inheritdoc />
        public Task<TResult> SingleAsync(IExecutor executor, ILogger logger = null) => Worker.SingleAsync(executor, null, logger);

        /// <inheritdoc />
        public TResult Single(IExecutor executor, ILogger logger = null) => Worker.Single(executor, null, logger);

        /// <inheritdoc />
        public Task<TResult> SingleOrDefaultAsync(IExecutor executor, ILogger logger = null) => Worker.SingleOrDefaultAsync(executor, null, logger);

        /// <inheritdoc />
        public TResult SingleOrDefault(IExecutor executor, ILogger logger = null) => Worker.SingleOrDefault(executor, null, logger);
    }
}
