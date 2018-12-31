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
            ISqlSyntax sqlSyntax, 
            SqlExecutor<TArgs, TResult> query, 
            LambdaExpression mapper, 
            ILogger logger)
        {
            var (wrappedBuilder, parameters) = query.ToSqlStatement();
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            var argsParam = mapper.Parameters.Count > 1 ? mapper.Parameters[1] : null;
            var state = new BuildMapState(query.PrimaryTableMember.memberName, parameters, mapper.Parameters[0], argsParam, wrappedStatement, query.SqlSyntax);
            var (resultType, properties, tables) = MapBuilder.BuildMapFromRoot(state, mapper.Body);

            switch (resultType)
            {
                case MapBuilder.MappingType.Map:
                {
                    properties = properties.Enumerate();
                    var statement = new MappedSelectStatement(properties, tables, wrappedStatement.Tables.First().RowNumberColumn);
                    var builder = new MappedSqlStatementBuilder(state, properties, statement, wrappedBuilder, sqlSyntax);
                    return builder.Compile<TArgs, TMapped>(statement, parameters.Parameters, sqlSyntax, QueryParseType.ORM);
                }
                case MapBuilder.MappingType.SimpleProp:
                {
                    properties = properties.Enumerate();
                    if (properties.Count() != 1)
                        throw new InvalidOperationException($"Expected one property, but got {properties.Count()}.");

                    // single property will not have an alias. Add one
                    var p = properties.First();
                    properties = new QueryElementBasedMappedProperty(
                        p.FromParams,
                        SqlStatementConstants.SingleColumnAlias,
                        p.MappedPropertyType,
                        p.PropertySegmentConstructors).ToEnumerable();

                    var statement = new MappedSelectStatement(properties, tables, wrappedStatement.Tables.First().RowNumberColumn);
                    var builder = new MappedSqlStatementBuilder(state, properties, statement, wrappedBuilder, sqlSyntax);
                    return builder
                        .CompileSimple<TArgs, TMapped>(statement, parameters.Parameters, sqlSyntax, SqlStatementConstants.SingleColumnAlias);
                }
                case MapBuilder.MappingType.SingleComplexProp:
                
                    // convert x => x to x => new X { x1 = x.x1, x2 = x.x2 }
                    // this is easier for mapper to understand
                    var init = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                        ReflectionUtils.ConvertToFullMemberInit(mapper.Body), 
                        mapper.Parameters);

                    return Compile<TArgs, TResult, TMapped>(sqlSyntax, query, init, logger: logger);

                case MapBuilder.MappingType.MultiComplexProp:

                    // convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
                    // this is easier for mapper to understand
                    var identityMap = Expression.Lambda(
                        ReflectionUtils.ConvertCollectionToFullMemberInit(typeof(TMapped), mapper.Body), 
                        mapper.Parameters);

                    return Compile<TArgs, TResult, TMapped>(sqlSyntax, query, identityMap, logger: logger);

                default:
                    throw new NotSupportedException(resultType.ToString());
            }
        }
    }
}
