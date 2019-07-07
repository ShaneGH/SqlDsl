namespace SqlDsl
{
    /// <summary>
    /// Settings required for mysql queries
    /// </summary>
    public class MySqlSettings
    {
        /// <summary>
        /// True if the version of MySql >= 8.
        /// If true, will provide some minor performace enhancements.
        /// Default false
        /// </summary>
        public bool Version8OrHigher { get; set; }

        public MySqlSettings()
        {
            Version8OrHigher = false;
        }

        public static readonly MySqlSettings Default = new MySqlSettings();
    }
}
