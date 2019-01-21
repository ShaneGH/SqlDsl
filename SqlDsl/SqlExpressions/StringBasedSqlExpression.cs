using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using SqlDsl.Mapper;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.SqlExpressions
{
    class StringBasedSqlExpression : SqlExpression<StringBasedElement>
    {        
        public StringBasedSqlExpression(Accumulator<StringBasedElement, BinarySqlOperator> acc)
            : base(acc)
        {
        }

        public override string BuildFromString(BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            var tableIsFirstParamPart = wrappedQueryAlias == null;

            var table1 = (First.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return Next.Aggregate(
                BuildColumn(table1, First),
                Aggregate);

            string Aggregate(string x, (StringBasedElement param, BinarySqlOperator type) y)
            {
                var table = (y.param.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param);

                return ISqlExpressionUtils.Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(string tab, StringBasedElement el)
            {
                var column = el.AddRoot(state).param;
                if (tableIsFirstParamPart)
                {
                    var p = column.Split('.');
                    if (p.Length > 1)
                    {
                        tab = p.Take(p.Length - 1).JoinString(".");
                        column = p[p.Length - 1];
                    }
                }

                return sqlFragmentBuilder.BuildSelectColumn(tab, column);
            }
        }
    }
    
    class SelectColumnBasedSqlExpression : SqlExpression<SelectColumnBasedElement>
    {        
        public SelectColumnBasedSqlExpression(Accumulator<SelectColumnBasedElement, BinarySqlOperator> acc)
            : base(acc)
        {
        }

        public override string BuildFromString(BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            return Next.Aggregate(
                BuildColumn(First),
                Aggregate);

            string Aggregate(string x, (SelectColumnBasedElement param, BinarySqlOperator type) y)
            {
                var yValue = BuildColumn(y.param);

                return ISqlExpressionUtils.Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(SelectColumnBasedElement el)
            {
                return sqlFragmentBuilder.BuildSelectColumn(
                    el.IsParameter ? null : wrappedQueryAlias, 
                    el.IsParameter ? el.ParameterName : el.Column.Alias);
            }
        }
    }
}