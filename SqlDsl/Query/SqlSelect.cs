using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
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

        public SqlSelect(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> tableProperty)
        {
            var (memberName, type) = CheckMemberExpression(tableProperty.Body, tableProperty.Parameters[0]);
            __PrimaryTableMember = (
                memberName,
                tableName ?? throw new ArgumentNullException(nameof(tableName)),
                type);

            return this;
        }

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From<TTable>(Expression<Func<TResult, TTable>> tableProperty) =>
            From<TTable>(typeof(TTable).Name, tableProperty);

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From(string tableName) =>
            From<TResult>(tableName, x => x);
    }
}