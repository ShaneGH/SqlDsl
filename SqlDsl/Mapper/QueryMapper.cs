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
using System.Threading.Tasks;

namespace SqlDsl.Mapper
{
    public static class QueryMapper
    {
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public static ICompiledQuery<TArgs, TMapped> Compile<TArgs, TResult, TMapped, TSqlBuilder>(
            ISqlFragmentBuilder sqlFragmentBuilder, 
            QueryBuilder<TSqlBuilder, TArgs, TResult> query, 
            LambdaExpression mapper, 
            ILogger logger = null)
            where TSqlBuilder: ISqlFragmentBuilder, new()
        {
            // TODO: filter columns
            // var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));

            var (wrappedBuilder, parameters) = query.ToSqlStatement(null);
            var mutableParameters = new ParamBuilder(parameters.ToList());
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            if (query.PrimaryTableMember == null)
            {
                throw new InvalidOperationException("The query must have at least one select table.");
            }

            var argsParam = mapper.Parameters.Count > 1 ? mapper.Parameters[1] : null;
            var state = new BuildMapState(query.PrimaryTableMember.Value.name, mutableParameters, mapper.Parameters[0], argsParam, wrappedStatement);
            var (resultType, properties, tables) = MapBuilder.BuildMapFromRoot(state, mapper.Body);

            switch (resultType)
            {
                case MapBuilder.BuildMapResult.Map:
                case MapBuilder.BuildMapResult.SimpleProp:
                    var requiredPropAliases = properties
                        .SelectMany(pms => pms.FromParams.GetEnumerable1())
                        .Where(x => x.paramRoot == state.QueryObject || state.ParameterRepresentsProperty.Any(y => y.parameter == x.paramRoot))
                        // TODO: using Accumulator.AddRoot here seems wrong
                        .Select(x => Accumulator.AddRoot(x.paramRoot, x.param, state))
                        .SelectMany(x => wrappedStatement.SelectColumns[x].ReferencesColumns.Select(y => y.table))
                        .Concat(tables.Select(t => t.From));

                    wrappedBuilder.FilterUnusedTables(requiredPropAliases);
                    wrappedStatement = new SqlStatement(wrappedBuilder);

                    if (resultType == MapBuilder.BuildMapResult.Map)
                        return ToSqlBuilder(sqlFragmentBuilder, properties, tables, wrappedBuilder, wrappedStatement, state)
                            .Compile<TArgs, TMapped>(mutableParameters.Parameters, QueryParseType.ORM);
                            
                    properties = properties.Enumerate();
                    if (properties.Count() != 1)
                    {
                        throw new InvalidOperationException($"Expected one property, but got {properties.Count()}.");
                    }

                    var p = properties.First();
                    return ToSqlBuilder(sqlFragmentBuilder, p.FromParams, p.MappedPropertyType, wrappedBuilder, wrappedStatement)
                        .CompileSimple<TArgs, TMapped>(mutableParameters.Parameters, SqlStatementConstants.SingleColumnAlias);

                case MapBuilder.BuildMapResult.SingleComplexProp:
                
                    // convert x => x to x => new X { x1 = x.x1, x2 = x.x2 }
                    // this is easier for mapper to understand
                    var init = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                        ReflectionUtils.ConvertToFullMemberInit(mapper.Body), 
                        mapper.Parameters);

                    return Compile<TArgs, TResult, TMapped, TSqlBuilder>(sqlFragmentBuilder, query, init, logger: logger);

                case MapBuilder.BuildMapResult.MultiComplexProp:

                    // convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
                    // this is easier for mapper to understand
                    var identityMap = Expression.Lambda(
                        AddMemberInitSelector(typeof(TMapped), mapper.Body), 
                        mapper.Parameters);

                    return Compile<TArgs, TResult, TMapped, TSqlBuilder>(sqlFragmentBuilder, query, identityMap, logger: logger);

                default:
                    throw new NotSupportedException(resultType.ToString());
            }
        }

        static SqlStatementBuilder ToSqlBuilder(ISqlFragmentBuilder sqlFragmentBuilder, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement, BuildMapState state)
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
                    fromParams: x.FromParams.GetEnumerable1().Select(Accumulator.AddRoot(state)),
                    to: x.To, 
                    propertySegmentConstructors: x.PropertySegmentConstructors));

            var builder = new SqlStatementBuilder(sqlFragmentBuilder);
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            foreach (var col in mappedValues)
            {
                // note: if AddSelectColumn is throwing an argument null exception
                // on alias, it probably means that a different ToSqlBuilder should be called
                
                var table = (col.from ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias;
                builder.AddSelectColumn(
                    col.type,
                    col.from,
                    col.to,
                    col.fromParams
                        .Select(p => (
                            (p ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias, 
                            p))
                    .ToArray(),
                    argConstructors: col.propertySegmentConstructors);
            }

            foreach (var col in rowIdPropertyMap)
                builder.RowIdsForMappedProperties.Add((col.rowIdColumnName, col.resultClassProperty));
                
            return builder;
        }

        static SqlStatementBuilder ToSqlBuilder(ISqlFragmentBuilder sqlFragmentBuilder, Accumulator property, Type cellDataType, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var builder = new SqlStatementBuilder(sqlFragmentBuilder);
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            var referencedColumns = new List<(string, string)>();
            string sql = null;
            string Add(string sqlPart, ExpressionType combiner)
            {
                if (!sqlPart.StartsWith("@"))
                {
                    referencedColumns.Add((wrappedStatement.UniqueAlias, sqlPart));
                    sqlPart = sqlFragmentBuilder.BuildSelectColumn(
                        wrappedStatement.UniqueAlias,
                        sqlPart);
                }

                return sql == null ?
                    sqlPart :
                    sqlFragmentBuilder.Concat(sql, sqlPart, combiner);
            }

            // second arg does not matter, as sql is null
            sql = Add(property.First.param, ExpressionType.ModuloAssign);
            foreach (var part in property.Next)
            {
                sql = Add(part.element.param, part.combiner);
            }

            builder.AddSelectColumn(
                cellDataType,
                sql,
                SqlStatementConstants.SingleColumnAlias,
                referencedColumns.ToArray());
                
            return builder;
        }

        /// <summary>
        /// convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
        /// </summary>
        static Expression AddMemberInitSelector(Type tMapped, Expression collection)
        {
            var enumeratedType = ReflectionUtils.GetIEnumerableType(tMapped);
            if (enumeratedType == null)
                throw new InvalidOperationException($"Expected type {tMapped} to implement IEnumerable<>");

            var innerParam = Expression.Parameter(enumeratedType);
            var mapperBody = ReflectionUtils.ConvertToFullMemberInit(innerParam);
            var mapper = Expression.Lambda(mapperBody, innerParam);

            return Expression.Call(
                ReflectionUtils
                    .GetMethod<IEnumerable<object>>(xs => xs.Select(x => x), enumeratedType, enumeratedType),
                collection,
                mapper);
        }
    }
}
