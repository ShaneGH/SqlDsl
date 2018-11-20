using System;

namespace SqlDsl.Utils
{
    /// <summary>
    /// A timer
    /// </summary>
    public struct Timer
    {
        private readonly DateTime StartTimeUtc;

        public Timer(bool dummy)
        {
            StartTimeUtc = DateTime.UtcNow;
        }

        public TimeSpan Split() => DateTime.UtcNow - StartTimeUtc;

        public string SplitString()
        {
            var time = Split();
            return $"{time.Seconds.ToString()}.{To3Digit(time.Milliseconds)}s";
        }

        static string To3Digit(int value)
        {
            if (value >= 100)
                return value.ToString();

            if (value >= 10)
                return $"0{value.ToString()}";

            return $"00{value.ToString()}";
        }
    }
}
