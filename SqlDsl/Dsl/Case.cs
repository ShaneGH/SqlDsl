namespace SqlDsl.Dsl
{
    /// <summary>
    /// Helps to create a sql case statement
    /// </summary>
    public interface ICase
    {
        /// <summary>
        /// The THEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ICase<T> Then<T>(T result);
    }

    /// <summary>
    /// Helps to create a sql case statement
    /// </summary>
    public interface ICase<T>
    {
        /// <summary>
        /// The WHEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ICaseResult<T> When(bool condition);
        
        /// <summary>
        /// The ELSE part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        T Else(bool condition);
    }

    /// <summary>
    /// Helps to create a sql case statement
    /// </summary>
    public interface ICaseResult<T>
    {
        /// <summary>
        /// The THEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ICase<T> Then(T result);
    }
}