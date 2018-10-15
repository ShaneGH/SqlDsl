using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.UnitTests.SqlFlavours
{
    public class TableDescriptor
    {
        public string Name;
        public IEnumerable<ColumnDescriptor> Columns;
    }

    public class ColumnDescriptor
    {
        public string Name;
        public Type DataType;
        public bool Nullable;
    }
}