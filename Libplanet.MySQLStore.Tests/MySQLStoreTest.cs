using System;
using Libplanet.Blockchain;
using Libplanet.Blockchain.Policies;
using Libplanet.Store;
using Libplanet.Tests.Blockchain;
using Libplanet.Tests.Common.Action;
using Libplanet.Tests.Store;
using Libplanet.Tests.Store.Trie;
using Xunit;

namespace Libplanet.MySQLStore.Tests
{
    public class MySQLStoreTest : StoreTest, IDisposable
    {
        private readonly MySQLStoreFixture _fx;

        public MySQLStoreTest()
        {
            try
            {
                Fx = _fx = new MySQLStoreFixture();
                FxConstructor = () => new MySQLStoreFixture();
            }
            catch (TypeInitializationException)
            {
                throw new SkipException("MySQL is not available.");
            }
        }

        public void Dispose()
        {
            _fx?.Dispose();
        }

        [SkippableFact]
        public void ReopenStoreAfterDispose()
        {
            MySQLStoreOptions options = new MySQLStoreOptions(
                "libplanet.mysql-test",
                "127.0.0.1",
                3306,
                "root",
                "root");

            try
            {
                var store = new MySQLStore(options);
                var stateStore =
                    new TrieStateStore(new MemoryKeyValueStore(), new MemoryKeyValueStore());
                var blocks = new BlockChain<DumbAction>(
                    new NullPolicy<DumbAction>(),
                    new VolatileStagePolicy<DumbAction>(),
                    store,
                    stateStore,
                    Fx.GenesisBlock
                );
                store.Dispose();
            }
            finally
            {
                var store = new MySQLStore(options);
                store.Dispose();
            }
        }
    }
}
