using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace SqlDsl.UnitTests.Utils
{
    public class TestSettings
    {
        public static readonly Settings Instance = LoadSettings();

        public static Settings LoadSettings()
        {
            // TODO: relative path
            using (var file = new FileStream(@"C:\Dev\SqlDsl\SqlDsl.UnitTests\testSettings.json", FileMode.OpenOrCreate, FileAccess.Read))
            using (var reader = new StreamReader(file))
            {
                return JsonConvert.DeserializeObject<Settings>(reader.ReadToEnd());
            }
        }
    }

    public class Settings
    {
        public Environments Environments { get; set;}
    }

    public class Environments
    {
        public bool Sqlite { get; set; } = true;
        public bool MySql { get; set; } = true;
    }
}