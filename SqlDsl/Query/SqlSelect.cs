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
        /// The name of the table in the SELECT statement
        /// </summary>
        string _PrimaryTableName;
        
        /// <inheritdoc />
        protected override string PrimaryTableName => _PrimaryTableName;
        
        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        (string name, Type type)? _PrimaryTableMember;
        
        /// <inheritdoc />
        public override (string name, Type type)? PrimaryTableMember => _PrimaryTableMember;

        public SqlSelect(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> tableProperty)
        {
            _PrimaryTableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _PrimaryTableMember = CheckMemberExpression(tableProperty.Body, tableProperty.Parameters[0]);

            return this;
        }

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From<TTable>(Expression<Func<TResult, TTable>> tableProperty) =>
            From<TTable>(typeof(TTable).Name, tableProperty);

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From(string tableName) =>
            From<TResult>(tableName, x => x);

        /// <inheritdoc />
        public IQuery<TArgs, TResult> From() =>
            From(typeof(TResult).Name);
    }
}