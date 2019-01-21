namespace SqlDsl.Dsl
{
    /// <summary>
    /// Helps to create a simple sql case statement
    /// </summary>
    public interface ISimpleCase<TSubject>
    {
        /// <summary>
        /// The WHEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ISimpleCaseResult<TSubject> When(TSubject option);
    }

    /// <summary>
    /// Helps to create a simple sql case statement
    /// </summary>
    public interface ISimpleCase<TSubject, TResult>
    {
        /// <summary>
        /// The ELSE part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        TResult Else(TResult option);
        
        /// <summary>
        /// The WHEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ISimpleCaseResult<TSubject, TResult> When(TSubject option);
    }

    /// <summary>
    /// Helps to create a simple sql case statement
    /// </summary>
    public interface ISimpleCaseResult<TSubject>
    {
        /// <summary>
        /// The THEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ISimpleCase<TSubject, TResult> Then<TResult>(TResult option);
    }

    /// <summary>
    /// Helps to create a simple sql case statement
    /// </summary>
    public interface ISimpleCaseResult<TSubject, TResult>
    {
        /// <summary>
        /// The THEN part of this case. 
        /// You can use any value available in the query object as an argument.
        /// However, you cannot use values from a table JOINED to the current mapping context. 
        /// You can only use values JOINED from the current mapping context.
        /// </summary>
        ISimpleCase<TSubject, TResult> Then(TResult option);
    }
}