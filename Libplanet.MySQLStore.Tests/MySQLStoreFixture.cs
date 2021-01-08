using Libplanet.Store;
using Libplanet.Store.Trie;
using Libplanet.Tests.Store;

namespace Libplanet.MySqlStore.Tests
{
    public class MySqlStoreFixture : StoreFixture
    {
        private readonly MySqlStore _store;

        public MySqlStoreFixture()
        {
            Options = new MySqlStoreOptions(
                "libplanet.mysql-test",
                "127.0.0.1",
                3306,
                "root",
                "root");

            _store = new MySqlStore(Options, blockCacheSize: 2, txCacheSize: 2);
            Store = _store;
            StateStore = LoadTrieStateStore(Options);
        }

        public MySqlStoreOptions Options { get; }

        public IStateStore LoadTrieStateStore(MySqlStoreOptions Options)
        {
            IKeyValueStore stateKeyValueStore =
                new MySqlKeyValueStore(Options, "State");
            IKeyValueStore stateHashKeyValueStore =
                new MySqlKeyValueStore(Options, "State_Hash");
            return new TrieStateStore(stateKeyValueStore, stateHashKeyValueStore);
        }

        public override void Dispose()
        {
            _store.Dispose();
        }
    }
}
