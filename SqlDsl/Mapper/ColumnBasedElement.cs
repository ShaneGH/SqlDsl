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
        public ISelectColumn RowIdColumn { get; }
        public readonly string Function;
        public readonly bool IsAggregated;

        public SelectColumnBasedElement(ISelectColumn column, ISelectColumn rowIdColumn, string function, bool isAggregated)
        {
            _Column = column ?? throw new ArgumentNullException(nameof(column));
            RowIdColumn = rowIdColumn ?? throw new ArgumentNullException(nameof(rowIdColumn));
            Function = function;
            IsAggregated = isAggregated;
            
            _ParameterName = null;
        }

        public SelectColumnBasedElement(string parameterName, ISelectColumn rowIdColumn, string function, bool isAggregated)
        {
            _ParameterName = parameterName ?? throw new ArgumentNullException(nameof(parameterName));
            RowIdColumn = rowIdColumn ?? throw new ArgumentNullException(nameof(rowIdColumn));
            Function = function;
            IsAggregated = isAggregated;
            
            _Column = null;
        }

        static T EnsureCol<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == false");
        
        static T EnsureParam<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == true");
    }
}