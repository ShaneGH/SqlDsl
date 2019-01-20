using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    public enum MappingPurpose
    {
        JoinOn,
        Where,
        OrderBy,
        Mapping
    }
}