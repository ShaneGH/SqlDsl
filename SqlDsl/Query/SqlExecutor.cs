using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
using SqlDsl.Schema;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    /// <summary>
    /// Build and execute sql queries
    /// </summary>
    public abstract class SqlExecutor<TArgs, TResult> : ISqlExecutor<TArgs, TResult>
    {      
        static readonly (string, string, Type) DefaultPrimaryTableMember = (
            SqlStatementConstants.RootObjectAlias, 
            TableAttribute.GetTableName(typeof(TResult)), 
            typeof(TResult));

        /// <summary>
        /// A cache of column names for a given type
        /// </summary>
        static readonly ConcurrentDictionary<Type, IEnumerable<(string nameX, string alias, Type dataType)>> Columns = new ConcurrentDictionary<Type, IEnumerable<(string, string, Type)>>();

        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        public abstract (string memberName, string tableName, Type type)? PrimaryTableDetails { get; }
        
        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        public (string memberName, string tableName, Type type) PrimaryTableMember => PrimaryTableDetails ?? DefaultPrimaryTableMember;
        
        /// <summary>
        /// The joins applied to the query
        /// </summary>
        public abstract IEnumerable<Join> Joins { get; }
        
        /// <summary>
        /// The paging for to the query
        /// </summary>
        protected abstract (Expression<Func<TArgs, int>> skip, Expression<Func<TArgs, int>> take) Paging { get; }

        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        protected abstract bool StrictJoins { get; }
        
        /// <summary>
        /// The WHERE part of the query
        /// </summary>
        protected abstract (ParameterExpression queryRoot, ParameterExpression args, Expression where)? WhereClause { get; }
        
        protected abstract IEnumerable<(ParameterExpression queryRoot, ParameterExpression args, Expression orderExpression, OrderDirection direction)> Ordering { get; }
        
        public readonly ISqlSyntax SqlSyntax;

        public SqlExecutor(ISqlSyntax sqlSyntax)
        {
            SqlSyntax = sqlSyntax;
        }
        
        /// <inheritdoc />
        public Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToIEnumerableAsync(executor, args, logger);

        /// <inheritdoc />
        public IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToIEnumerable(executor, args, logger);
            
        /// <inheritdoc />
        public Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToListAsync(executor, args, logger);

        /// <inheritdoc />
        public List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToList(executor, args, logger);
            
        /// <inheritdoc />
        public Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToArrayAsync(executor, args, logger);

        /// <inheritdoc />
        public TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToArray(executor, args, logger);

        /// <inheritdoc />
        public TResult First(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).First(executor, args, logger);

        /// <inheritdoc />
        public Task<TResult> FirstAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).FirstAsync(executor, args, logger);

        /// <inheritdoc />
        public TResult FirstOrDefault(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).FirstOrDefault(executor, args, logger);

        /// <inheritdoc />
        public Task<TResult> FirstOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).FirstOrDefaultAsync(executor, args, logger);

        /// <inheritdoc />
        public TResult Single(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).Single(executor, args, logger);

        /// <inheritdoc />
        public Task<TResult> SingleAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).SingleAsync(executor, args, logger);

        /// <inheritdoc />
        public TResult SingleOrDefault(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).SingleOrDefault(executor, args, logger);

        /// <inheritdoc />
        public Task<TResult> SingleOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).SingleOrDefaultAsync(executor, args, logger);

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TArgs, TResult> Compile(ILogger logger = null)
        {
            var timer = new Timer(true);
            var (builder, paramaters) = ToSqlStatement();
            var compiled = builder
                .Compile<TArgs, TResult>(new SqlStatement(builder), paramaters.Parameters, builder.SqlSyntax, QueryParseType.DoNotDuplicate, false);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return compiled;
        }

        /// <summary>
        /// Create a populated sql builder along with any constants specified in the query
        /// </summary>
        /// <param name="filterSelectCols">If specified, only add the given columns to the SELECT statement</param>
        public (SqlStatementBuilder builder, ParamBuilder paramaters) ToSqlStatement()
        {
            var (memberName, tableName, primaryTableMemberType) = PrimaryTableMember;

            // get all columns from SELECT and JOINs
            var primaryTableColumns = ColumnsOf(primaryTableMemberType);
            var joinsWithColNames = Joins
                .Select(j => (join: j, Columns: ColumnsOf(j.JoinedTableProperty.type)))
                .ToList();

            var selectColumns = joinsWithColNames
                .SelectMany((x, i) => x.Columns
                    .Select(y => (table: x.join.JoinedTableProperty.name, column: y)))
                .Concat(primaryTableColumns
                    .Select(y => (table: memberName, column: y)));
            
            // Set the SELECT table
            var builder = new SqlStatementBuilder(
                SqlSyntax, 
                tableName, 
                memberName, 
                primaryTableColumns.Select(c => c.name),
                StrictJoins);

            // add each join
            var param = new ParamBuilder();
            foreach (var join in joinsWithColNames)
            {
                builder.AddJoin(
                    join.join.JoinType, 
                    join.join.TableName, 
                    join.Columns.Select(c => c.name),
                    join.join.JoinExpression.rootObjectParam,
                    join.join.JoinExpression.queryArgs,
                    join.join.JoinExpression.joinParam,
                    join.join.JoinExpression.joinExpression,
                    param,
                    join.join.JoinedTableProperty.name,
                    join.join.JoinParent);
            }

            // Add select columns to builder
            foreach (var col in selectColumns)
            {   
                var alias = col.table == SqlStatementConstants.RootObjectAlias ? col.column.alias : $"{col.table}.{col.column.alias}";
                builder.AddSelectColumn(col.column.dataType, col.table, col.column.name, alias);
            }

            // add a where clause if specified
            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.args, WhereClause.Value.where, param);

            // add order by if specified
            foreach (var (queryRoot, args, orderExpression, direction) in Ordering)
                builder.AddOrderBy(queryRoot, args, orderExpression, direction, param);

            // add paging if necessary
            SetPaging(builder, param);

            return (builder, param);
        }

        void SetPaging(SqlStatementBuilder builder, ParamBuilder param)
        {
            var argsParam = Paging.skip != null
                ? Paging.skip.Parameters[0]
                : Paging.take != null
                    ? Paging.take.Parameters[0]
                    : null;

            if (argsParam == null)
                return;

            Expression skipWithParametersReplaced = null;
            Expression skip = null;
            if (Paging.skip != null)
            {
                skipWithParametersReplaced = ParameterReplacer.ReplaceParameter(Paging.skip.Body, Paging.skip.Parameters[0], argsParam);
                skip = Expression.LessThan(
                    skipWithParametersReplaced, 
                    Expression.Call(
                        ReflectionUtils.GetOrderByRowNumberMethod()));
            }

            Expression take = null;
            if (Paging.take != null)
            {
                take = ParameterReplacer.ReplaceParameter(Paging.take.Body, Paging.take.Parameters[0], argsParam);
                if (skipWithParametersReplaced != null)
                    take = Expression.Add(skipWithParametersReplaced, take);

                take = Expression.LessThanOrEqual(
                    Expression.Call(
                        ReflectionUtils.GetOrderByRowNumberMethod()), 
                    take);
            }

            var paging = new[]
            {
                skip, 
                take
            }
            .RemoveNulls()
            .Aggregate(Expression.AndAlso);

            builder.AddPaging(paging, argsParam, param);
        }
        
        /// <summary>
        /// Get all of the column names for a given type
        /// </summary>
        static IEnumerable<(string name, string alias, Type dataType)> ColumnsOf(Type t)
        {
            if (Columns.TryGetValue(t, out IEnumerable<(string, string, Type)> value))
                return value;

            value = GetColumnNames(t)
                .ToList()
                .AsReadOnly();
                
            return Columns.GetOrAdd(t, value);
        }

        /// <summary>
        /// Return all of the property and field names of a type as column names
        /// </summary>
        static IEnumerable<(string name, string alias, Type dataType)> GetColumnNames(Type t)
        {
            foreach (var col in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                yield return ColumnAttribute.GetColumnName(col).AddT(col.PropertyType);
                
            foreach (var col in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
                yield return ColumnAttribute.GetColumnName(col).AddT(col.FieldType);
        }
    }
}