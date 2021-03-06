using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
using SqlDsl.Schema;
using SqlDsl.SqlBuilders;
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
    /// Class to kick off a sql statement
    /// </summary>
    public class SqlSelect<TArgs, TResult> : Query<TArgs, TResult>, IOrderer<TArgs, TResult>, ISqlSelect<TArgs, TResult>
    {        
        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        (string name, string tableName, Type type)? __PrimaryTableMember;
        
        /// <inheritdoc />
        public override (string memberName, string tableName, Type type)? PrimaryTableDetails => __PrimaryTableMember;

        /// <inheritdoc />
        protected override bool StrictJoins { get; }

        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        public SqlSelect(ISqlSyntax sqlSyntax, bool strictJoins)
            : base(sqlSyntax)
        {
            StrictJoins = strictJoins;
        }

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From<TTable>(Expression<Func<TResult, TTable>> tableProperty)
        {
            var (memberName, type) = CheckMemberExpression(tableProperty.Body, tableProperty.Parameters[0]);
            
            __PrimaryTableMember = (
                memberName,
                TableAttribute.GetTableName(type),
                type);

            return this;
        }
    }
}