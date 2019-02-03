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
            bool requiresPropertyUnwrap,
            ILogger logger)
        {
            var (wrappedBuilder, parameters) = query.ToSqlStatement();
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            var argsParam = mapper.Parameters.Count > 1 ? mapper.Parameters[1] : null;
            var state = new BuildMapState(query.PrimaryTableMember.memberName, parameters, mapper.Parameters[0], argsParam, wrappedStatement, query.SqlSyntax, true, MappingPurpose.Mapping);
            var (properties, tables) = MapBuilder.BuildMapFromRoot(state, mapper.Body);

            properties = properties.Enumerate();
            var statement = new MappedSelectStatement(properties, tables, wrappedStatement.Tables.First().RowNumberColumn);
            var builder = new MappedSqlStatementBuilder(state, properties, statement, wrappedBuilder, sqlSyntax);
            return builder.Compile<TArgs, TMapped>(statement, parameters.Parameters, sqlSyntax, QueryParseType.ORM, requiresPropertyUnwrap);
        }
    }
}
