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
                case MapBuilder.MappingType.SimpleProp:
                    // var requiredColumns = properties
                    //     .SelectMany(pms => pms.FromParams.GetEnumerable1())
                    //     .Where(x => x.ParamRoot == state.QueryObject || state.ParameterRepresentsProperty.Any(y => y.parameter == x.ParamRoot))
                    //     // TODO: using Accumulator.AddRoot here seems wrong
                    //     .Select(x => x.AddRoot(state))
                    //     .Select(x => wrappedStatement.SelectColumns.TryGetColumn(x.param))
                    //     .RemoveNulls()
                    //     .SelectMany(x => x.ReferencesColumns.Select(y => y.table))
                    //     .Concat(tables.Select(t => t.From));

                    // var wow = properties
                    //     .Select(xx => xx.Convert(state))
                    //     .ToArray();

                    // wrappedBuilder.FilterUnusedTables(requiredPropAliases);
                    // wrappedStatement = new SqlStatement(wrappedBuilder);
                    
                    // var filteredWrappedBuilder = new FilteredSqlStatementBuilder(wrappedBuilder, requiredColumns);
                    // wrappedStatement = new SqlStatement(filteredWrappedBuilder);

                    if (resultType == MapBuilder.MappingType.Map)
                    {
                        var b = ToSqlBuilder(sqlFragmentBuilder, properties, tables, wrappedBuilder, wrappedStatement, state);
                        return b.Compile<TArgs, TMapped>(new SqlStatement(b), mutableParameters.Parameters, b.SqlSyntax, QueryParseType.ORM);
                    }
                            
                    properties = properties.Enumerate();
                    if (properties.Count() != 1)
                    {
                        throw new InvalidOperationException($"Expected one property, but got {properties.Count()}.");
                    }

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

        static MappedSqlStatementBuilder ToSqlBuilder(ISqlSyntax sqlFragmentBuilder, IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<MappedTable> tables, ISqlString wrappedBuilder, ISqlStatement wrappedStatement, BuildMapState state)
        {
            var rowIdPropertyMap = tables
                // if mapping does not map to a specific property (e.g. q => q.Args.Select(a => new object()))
                // To will be null
                .Where(t => t.To != null)
                .Select(t => (rowIdColumnName: $"{t.From}.{SqlStatementConstants.RowIdName}", resultClassProperty: t.To))
                .Enumerate();

            var mappedValues = properties
                .Select(x => (
                    type: x.MappedPropertyType, 
                    from: x.FromParams.BuildFromString(state, sqlFragmentBuilder, wrappedStatement.UniqueAlias),
                    fromParams: x.FromParams
                        .GetEnumerable1()
                        .Where(p => !p.IsParameter)
                        .Select(p => (
                            sc: (table: wrappedStatement.UniqueAlias, column: p.Column.Alias), 
                            aT: p.ColumnIsAggregatedToDifferentTable
                                ? wrappedStatement.GetTableForColum(p.RowIdColumn.Alias).Alias
                                : null)
                        )
                        .ToArray(),
                    to: x.To, 
                    propertySegmentConstructors: x.PropertySegmentConstructors));

            var allProperties = mappedValues.SelectMany(x => x.fromParams);

            var builder = new MappedSqlStatementBuilder(sqlFragmentBuilder, wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            foreach (var col in mappedValues)
            {
                // note: if AddSelectColumn is throwing an argument null exception
                // on alias, it probably means that a different ToSqlBuilder overload should be called
                
                var table = (col.from ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias;
                builder.AddSelectColumn(
                    col.type,
                    col.from,
                    col.to,
                    col.fromParams.Select(p => (p.sc.table, p.sc.column, p.aT)).ToArray(),
                    argConstructors: col.propertySegmentConstructors);
            }

            foreach (var col in rowIdPropertyMap)
                builder.RowIdsForMappedProperties.Add((col.rowIdColumnName, col.resultClassProperty));
                
            return builder;
        }

        static MappedSqlStatementBuilder ToSqlBuilder(
            ISqlSyntax sqlFragmentBuilder, 
            IAccumulator<TheAmazingElement> property, 
            Type cellDataType, 
            ISqlString wrappedBuilder, 
            ISqlStatement wrappedStatement, 
            BuildMapState state)
        {
            var builder = new MappedSqlStatementBuilder(sqlFragmentBuilder, wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            var referencedColumns = property
                .GetEnumerable1()
                .Where(x => !x.IsParameter)
                .Select(x => (
                    wrappedStatement.UniqueAlias, 
                    x.Column.Alias,
                    x.ColumnIsAggregatedToDifferentTable
                        ? wrappedStatement.GetTableForColum(x.RowIdColumn.Alias).Alias
                        : null))
                .ToArray();

            var sql = property.BuildFromString(state, sqlFragmentBuilder, wrappedStatement.UniqueAlias);
            builder.AddSelectColumn(
                cellDataType,
                sql,
                SqlStatementConstants.SingleColumnAlias,
                referencedColumns);
                
            return builder;
        }
    }
}
