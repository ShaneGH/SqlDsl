using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
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
        static readonly (string, string, Type) DefaultPrimaryTableMember = (SqlStatementConstants.RootObjectAlias, typeof(TResult).Name, typeof(TResult));

        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        public abstract (string memberName, string tableName, Type type)? PrimaryTableDetauls { get; }
        
        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        public (string memberName, string tableName, Type type) PrimaryTableMember => PrimaryTableDetauls ?? DefaultPrimaryTableMember;
        
        /// <summary>
        /// The joins applied to the query
        /// </summary>
        public abstract IEnumerable<Join> Joins { get; }
        
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

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TArgs, TResult> Compile(ILogger logger = null)
        {
            var timer = new Timer(true);
            var (builder, paramaters) = ToSqlStatement();
            var compiled = builder
                .Compile<TArgs, TResult>(new SqlStatement(builder), paramaters.Parameters, builder.SqlSyntax, QueryParseType.DoNotDuplicate);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return compiled;
        }

        static readonly string NullString = null;

        /// <summary>
        /// Create a populated sql builder along with any constants specified in the query
        /// </summary>
        /// <param name="filterSelectCols">If specified, only add the given columns to the SELECT statement</param>
        public (SqlStatementBuilder builder, ParamBuilder paramaters) ToSqlStatement()
        {
            var (memberName, tableName, primaryTableMemberType) = PrimaryTableMember;
            
            // Set the SELECT table
            var builder = new SqlStatementBuilder(SqlSyntax, tableName, memberName);

            // get all columns from SELECT and JOINs
            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinedTableProperty.name, column: y)))
                .Concat(ColumnsOf(primaryTableMemberType)
                    .Select(y => (table: memberName, column: y)));

            // add each join
            var param = new ParamBuilder();
            foreach (var join in Joins)
            {
                builder.AddJoin(
                    join.JoinType, 
                    join.TableName, 
                    join.JoinExpression.rootObjectParam,
                    join.JoinExpression.queryArgs,
                    join.JoinExpression.joinParam,
                    join.JoinExpression.joinExpression,
                    param,
                    join.JoinedTableProperty.name);
            }

            // Add select columns to builder
            foreach (var col in selectColumns)
            {
                var alias = col.table == SqlStatementConstants.RootObjectAlias ? col.column.name : $"{col.table}.{col.column.name}";
                builder.AddSelectColumn(col.column.dataType, SqlSyntax.BuildSelectColumn(col.table, col.column.name), alias, new [] {(col.table, col.column.name, NullString)});
            }

            // add a where clause if specified
            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.args, WhereClause.Value.where, param);

            // add order by if specified
            foreach (var (queryRoot, args, orderExpression, direction) in Ordering)
                builder.AddOrderBy(queryRoot, args, orderExpression, direction, param);

            return (builder, param);
        }

        /// <summary>
        /// A cache of column names for a given type
        /// </summary>
        static readonly ConcurrentDictionary<Type, IEnumerable<(string name, Type dataType)>> Columns = new ConcurrentDictionary<Type, IEnumerable<(string, Type)>>();
        
        /// <summary>
        /// Get all of the column names for a given type
        /// </summary>
        static IEnumerable<(string name, Type dataType)> ColumnsOf(Type t)
        {
            if (Columns.TryGetValue(t, out IEnumerable<(string, Type)> value))
                return value;

            value = GetColumnNames(t)
                .ToList()
                .AsReadOnly();
                
            return Columns.GetOrAdd(t, value);
        }

        /// <summary>
        /// Return all of the property and field names of a type as column names
        /// </summary>
        static IEnumerable<(string name, Type dataType)> GetColumnNames(Type t)
        {
            foreach (var col in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                yield return (col.Name, col.PropertyType);
                
            foreach (var col in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
                yield return (col.Name, col.FieldType);
        }
    }
}