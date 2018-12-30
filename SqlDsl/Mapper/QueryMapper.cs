using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SqlDsl.Mapper
{
    public static class QueryMapper
    {
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public static ICompiledQuery<TArgs, TMapped> Compile<TArgs, TResult, TMapped>(
            ISqlSyntax sqlFragmentBuilder, 
            SqlExecutor<TArgs, TResult> query, 
            LambdaExpression mapper, 
            ILogger logger)
        {
            var (wrappedBuilder, parameters) = query.ToSqlStatement();
            var mutableParameters = new ParamBuilder(parameters.ToList());
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            if (query.PrimaryTableMember == null)
                throw new InvalidOperationException("The query must have at least one select table.");

            var argsParam = mapper.Parameters.Count > 1 ? mapper.Parameters[1] : null;
            var state = new BuildMapState(query.PrimaryTableMember.Value.name, mutableParameters, mapper.Parameters[0], argsParam, wrappedStatement, query.SqlSyntax);
            var (resultType, properties, tables) = MapBuilder.BuildMapFromRoot(state, mapper.Body);

            switch (resultType)
            {
                case MapBuilder.MappingType.Map:
                    var statement = new SqlSelectStatement(properties, tables, wrappedStatement.Tables.First().RowNumberColumn);
                    var builder = new MappedSqlStatementBuilder(state, properties, statement, wrappedBuilder, sqlFragmentBuilder);
                    return builder.Compile<TArgs, TMapped>(statement, mutableParameters.Parameters, sqlFragmentBuilder, QueryParseType.ORM);
                            
                case MapBuilder.MappingType.SimpleProp:
                    properties = properties.Enumerate();
                    if (properties.Count() != 1)
                        throw new InvalidOperationException($"Expected one property, but got {properties.Count()}.");

                    var p = properties.First();
                    return ToSqlBuilder(sqlFragmentBuilder, p.FromParams, p.MappedPropertyType, wrappedBuilder, wrappedStatement, state)
                        .CompileSimple<TArgs, TMapped>(mutableParameters.Parameters, SqlStatementConstants.SingleColumnAlias);

                case MapBuilder.MappingType.SingleComplexProp:
                
                    // convert x => x to x => new X { x1 = x.x1, x2 = x.x2 }
                    // this is easier for mapper to understand
                    var init = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                        ReflectionUtils.ConvertToFullMemberInit(mapper.Body), 
                        mapper.Parameters);

                    return Compile<TArgs, TResult, TMapped>(sqlFragmentBuilder, query, init, logger: logger);

                case MapBuilder.MappingType.MultiComplexProp:

                    // convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
                    // this is easier for mapper to understand
                    var identityMap = Expression.Lambda(
                        ReflectionUtils.ConvertCollectionToFullMemberInit(typeof(TMapped), mapper.Body), 
                        mapper.Parameters);

                    return Compile<TArgs, TResult, TMapped>(sqlFragmentBuilder, query, identityMap, logger: logger);

                default:
                    throw new NotSupportedException(resultType.ToString());
            }
        }

        static MappedSqlStatementBuilder ToSqlBuilder(
            ISqlSyntax sqlFragmentBuilder, 
            IAccumulator<TheAmazingElement> property, 
            Type cellDataType, 
            ISqlString wrappedBuilder, 
            ISqlStatement wrappedStatement, 
            BuildMapState state)
        {
            throw new NotImplementedException();
            // var builder = new MappedSqlStatementBuilder(sqlFragmentBuilder, wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            // var referencedColumns = property
            //     .GetEnumerable1()
            //     .Where(x => !x.IsParameter)
            //     .Select(x => (
            //         wrappedStatement.UniqueAlias, 
            //         x.Column.Alias,
            //         x.ColumnIsAggregatedToDifferentTable
            //             ? wrappedStatement.GetTableForColum(x.RowIdColumn.Alias).Alias
            //             : null))
            //     .ToArray();

            // var sql = property.BuildFromString(state, sqlFragmentBuilder, wrappedStatement.UniqueAlias);
            // builder.AddSelectColumn(
            //     cellDataType,
            //     sql,
            //     SqlStatementConstants.SingleColumnAlias,
            //     referencedColumns);
                
            // return builder;
        }
    }
}
