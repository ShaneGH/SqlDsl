using System;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Mapper
{
    struct SelectColumnBasedElement
    {
        public bool IsParameter => _ParameterName != null;
        readonly string _ParameterName;
        public string ParameterName => EnsureParam(_ParameterName);
        readonly ISelectColumn _Column;
        public ISelectColumn Column => EnsureCol(_Column);
        readonly ISelectColumn _RowIdColumn;
        public ISelectColumn RowIdColumn => EnsureCol(_RowIdColumn);
        public readonly string Function;

        /// <summary>
        /// If true, the colum and row id columns come from different tables
        /// </summary>
        public bool ColumnIsAggregatedToDifferentTable => Column.Table != RowIdColumn.Table;

        public SelectColumnBasedElement(ISelectColumn column, ISelectColumn rowIdColumn, string function)
        {
            _Column = column ?? throw new ArgumentNullException(nameof(column));
            _RowIdColumn = rowIdColumn ?? throw new ArgumentNullException(nameof(rowIdColumn));
            Function = function;
            
            _ParameterName = null;
        }

        public SelectColumnBasedElement(string parameterName, string function)
        {
            _ParameterName = parameterName ?? throw new ArgumentNullException(nameof(parameterName));
            Function = function;
            
            _Column = null;
            _RowIdColumn = null;
        }

        static T EnsureCol<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == false");
        
        static T EnsureParam<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == true");
    }
}