using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.MySql;

namespace SqlDsl
{
    /// <summary>
    /// Settings required for mysql queries
    /// </summary>
    public class MySqlSettings
    {
        /// <summary>
        /// True if the version of MySql >= 8.
        /// If true, will provide some minor performace enhancements
        /// </summary>
        public bool Version8OrHigher { get; set; }

        public static readonly MySqlSettings Default = new MySqlSettings
        {
            Version8OrHigher = false
        };
    }
}
