using System;
using Libplanet.Blockchain;
using Libplanet.Blockchain.Policies;
using Libplanet.Store;
using Libplanet.Tests.Blockchain;
using Libplanet.Tests.Common.Action;
using Libplanet.Tests.Store;
using Libplanet.Tests.Store.Trie;
using Xunit;

namespace Libplanet.MySqlStore.Tests
{
    public class MySqlStoreTest : StoreTest, IDisposable
    {
        private readonly MySqlStoreFixture _fx;

        public MySqlStoreTest()
        {
            try
            {
                Fx = _fx = new MySqlStoreFixture();
                FxConstructor = () => new MySqlStoreFixture();
            }
            catch (TypeInitializationException)
            {
                throw new SkipException("MySql is not available.");
            }
        }

        public void Dispose()
        {
            _fx?.Dispose();
        }

        [SkippableFact]
        public void ReopenStoreAfterDispose()
        {
            MySqlStoreOptions options = new MySqlStoreOptions(
                "libplanet.mysql-test",
                "127.0.0.1",
                3306,
                "root",
                "root");

            try
            {
                var store = new MySqlStore(options);
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
                var store = new MySqlStore(options);
                store.Dispose();
            }
        }
    }
}
