using System;
using Libplanet.Tests.Store.Trie;
using Xunit;

namespace Libplanet.MySqlStore.Tests
{
    public class MySqlKeyValueStoreTest : KeyValueStoreTest, IDisposable
    {
        private readonly MySqlKeyValueStore _MySqlKeyValueStore;

        public MySqlKeyValueStoreTest()
        {
            MySqlStoreOptions options = new MySqlStoreOptions(
                "libplanet.mysql-test",
                "127.0.0.1",
                3306,
                "root",
                "root");

            try
            {
                KeyValueStore = _MySqlKeyValueStore = new MySqlKeyValueStore(
                    options,
                    "Test");
                InitializePreStoredData();
            }
            catch (TypeInitializationException)
            {
                throw new SkipException("MySql is not available.");
            }
        }

        public void Dispose()
        {
            _MySqlKeyValueStore.Dispose();
        }
    }
}
