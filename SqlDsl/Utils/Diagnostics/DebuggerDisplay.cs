using System.Diagnostics;

namespace SqlDsl.Utils.Diagnostics
{
    interface IDebuggerDisplay
    {
        string GetDebuggerDisplay();
    }

    public static class IDebuggerDisplayUtils
    {
        public static string GetDebuggerDisplay(this object input)
        {
            if (input == null)
                return "null";

            if (input is IDebuggerDisplay)
                return (input as IDebuggerDisplay).GetDebuggerDisplay();

            return input.ToString();
        }
    }
}