using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
            var settingsDir = Directory.GetCurrentDirectory();

            using (var baseFile = new FileStream(Path.Combine(settingsDir, "testSettings.json"), FileMode.OpenOrCreate, FileAccess.Read))
            using (var baseReader = new StreamReader(baseFile))
            using (var userFile = new FileStream(Path.Combine(settingsDir, "testSettings.user.json"), FileMode.OpenOrCreate, FileAccess.Read))
            using (var userReader = new StreamReader(userFile))
            {
                JObject bs = JObject.Parse(baseReader.ReadToEnd());
                JObject ur = JObject.Parse(userReader.ReadToEnd());

                bs.Merge(ur, new JsonMergeSettings
                {
                    // union array values together to avoid duplicates
                    MergeArrayHandling = MergeArrayHandling.Union
                });

                return JsonConvert.DeserializeObject<Settings>(bs.ToString());
            }
        }
    }

    public class Settings
    {
        public Environments Environments { get; set;}
        public string MySqlConnectionString { get; set; }
        public string TSqlConnectionString { get; set; }
        public bool MySqlV8 { get; set; }
    }

    public class Environments
    {
        public bool Sqlite { get; set; } = true;
        public bool MySql { get; set; } = true;
        public bool TSql { get; set; } = true;
    }
}