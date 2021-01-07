using System;
using Libplanet.Tests.Store.Trie;
using Xunit;

namespace Libplanet.MySQLStore.Tests
{
    public class MySQLKeyValueStoreTest : KeyValueStoreTest, IDisposable
    {
        private readonly MySQLKeyValueStore _MySQLKeyValueStore;

        public MySQLKeyValueStoreTest()
        {
            MySQLStoreOptions options = new MySQLStoreOptions(
                "libplanet.mysql-test",
                "127.0.0.1",
                3306,
                "root",
                "root");

            try
            {
                KeyValueStore = _MySQLKeyValueStore = new MySQLKeyValueStore(
                    options,
                    "Test");
                InitializePreStoredData();
            }
            catch (TypeInitializationException)
            {
                throw new SkipException("MySQL is not available.");
            }
        }

        public void Dispose()
        {
            _MySQLKeyValueStore.Dispose();
        }
    }
}
