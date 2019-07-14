using System;
using System.Diagnostics;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.Mapper
{
    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    struct SelectColumnBasedElement : IDebuggerDisplay
    {
        public bool IsParameter => _ParameterName != null;
        readonly string _ParameterName;
        public string ParameterName => EnsureParam(_ParameterName);
        readonly ISelectColumn _Column;
        public ISelectColumn Column => EnsureCol(_Column);
        public ICompositeKey PrimaryKey { get; }

        public SelectColumnBasedElement(ISelectColumn column, ICompositeKey primaryKey)
        {
            _Column = column ?? throw new ArgumentNullException(nameof(column));
            PrimaryKey = primaryKey ?? throw new ArgumentNullException(nameof(primaryKey));
            
            _ParameterName = null;
        }

        public SelectColumnBasedElement(string parameterName, ICompositeKey primaryKey)
        {
            _ParameterName = parameterName ?? throw new ArgumentNullException(nameof(parameterName));
            PrimaryKey = primaryKey ?? throw new ArgumentNullException(nameof(primaryKey));
            
            _Column = null;
        }

        static T EnsureCol<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == false");
        
        static T EnsureParam<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == true");

        public string GetDebuggerDisplay()
        {
            if (IsParameter)
                return ParameterName;

            return Column.Alias;
        }
    }
}