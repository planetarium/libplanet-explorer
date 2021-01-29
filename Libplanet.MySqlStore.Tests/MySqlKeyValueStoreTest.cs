using System;
using Libplanet.Tests.Store.Trie;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Libplanet.MySqlStore.Tests
{
    public class MySqlKeyValueStoreTest : KeyValueStoreTest
    {
        private readonly MySqlKeyValueStore _MySqlKeyValueStore;

        public MySqlKeyValueStoreTest()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("AppSettings.json").Build();
            var dbConfig = config.Get<DatabaseConfig>();

            MySqlStoreOptions options = new MySqlStoreOptions(
                dbConfig.Database,
                dbConfig.Server,
                dbConfig.Port,
                dbConfig.UserId,
                dbConfig.Password);

            try
            {
                KeyValueStore = _MySqlKeyValueStore = new MySqlKeyValueStore(
                    options,
                    "Test");
                InitializePreStoredData();
            }
            catch (TypeInitializationException)
            {
                throw new SkipException("MySQL is not available.");
            }
        }

        public class DatabaseConfig
        {
            public string Database { get; set; }

            public string Server { get; set; }

            public uint Port { get; set; }

            public string UserId { get; set; }

            public string Password { get; set; }
        }
    }
}
