using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Helper to compile functions which will convert an object graph into an anonymous object
    /// </summary>
    public static class AnonymousObjects
    {
        /// <summary>
        /// Compile a function to build an anonymous object from an object graph
        /// </summary>
        public static Func<ObjectGraph, ILogger, T> CompileAnonymousObjectBuilder<T>()
        {
            throw new NotImplementedException("Anonymous object maps are not yet supported");
        }
    }
}
