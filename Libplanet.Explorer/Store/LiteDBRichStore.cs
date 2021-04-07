using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Bencodex.Types;
using Libplanet.Action;
using Libplanet.Blocks;
using Libplanet.Store;
using Libplanet.Tx;
using LiteDB;
using LruCacheNet;
using FileMode = LiteDB.FileMode;

namespace Libplanet.Explorer.Store
{
    // It assumes running Explorer as online-mode.
    public class LiteDBRichStore : IRichStore
    {
        private const string TxRefCollectionName = "block_ref";
        private const string SignerRefCollectionName = "signer_ref";
        private const string UpdatedAddressRefCollectionName = "updated_address_ref";

        private readonly MemoryStream _memoryStream;
        private readonly LiteDatabase _db;
        private readonly LruCache<HashDigest<SHA256>, BlockDigest> _blockCache;

        // FIXME we should separate it.
        private readonly IStore _store;

        public LiteDBRichStore(
            IStore store,
            string path,
            bool journal = true,
            int indexCacheSize = 50000,
            bool flush = true,
            bool readOnly = false)
        {
            _store = store;

            if (path is null)
            {
                _memoryStream = new MemoryStream();
                _db = new LiteDatabase(_memoryStream);
            }
            else
            {
                var connectionString = new ConnectionString
                {
                    Filename = Path.Combine(path, "ext.ldb"),
                    Journal = journal,
                    CacheSize = indexCacheSize,
                    Flush = flush,
                };

                if (readOnly)
                {
                    connectionString.Mode = FileMode.ReadOnly;
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) &&
                         Type.GetType("Mono.Runtime") is null)
                {
                    // macOS + .NETCore doesn't support shared lock.
                    connectionString.Mode = FileMode.Exclusive;
                }

                _db = new LiteDatabase(connectionString);

                lock (_db.Mapper)
                {
                    _db.Mapper.RegisterType(
                        hash => hash.ToByteArray(),
                        b => new HashDigest<SHA256>(b));
                    _db.Mapper.RegisterType(
                        txid => txid.ToByteArray(),
                        b => new TxId(b));
                    _db.Mapper.RegisterType(
                        address => address.ToByteArray(),
                        b => new Address(b.AsBinary));
                }

                _blockCache = new LruCache<HashDigest<SHA256>, BlockDigest>(capacity: 512);
            }
        }

        /// <inheritdoc cref="IStore"/>
        public long? GetBlockIndex(HashDigest<SHA256> blockHash)
        {
            return _store.GetBlockIndex(blockHash);
        }

        public DateTimeOffset? GetBlockPerceivedTime(HashDigest<SHA256> blockHash)
        {
            return _store.GetBlockPerceivedTime(blockHash);
        }

        public BlockDigest? GetBlockDigest(HashDigest<SHA256> blockHash)
        {
            if (_blockCache.TryGetValue(blockHash, out BlockDigest cachedDigest))
            {
                return cachedDigest;
            }

            var blockDigest = _store.GetBlockDigest(blockHash);

            if (!(blockDigest is null))
            {
                _blockCache.AddOrUpdate(blockHash, blockDigest.Value);
            }

            return blockDigest;
        }

        /// <inheritdoc cref="IStore"/>
        public bool DeleteBlock(HashDigest<SHA256> blockHash)
        {
            _blockCache.Remove(blockHash);

            return _store.DeleteBlock(blockHash);
        }

        /// <inheritdoc cref="IStore"/>
        public bool ContainsBlock(HashDigest<SHA256> blockHash)
        {
            if (_blockCache.ContainsKey(blockHash))
            {
                return true;
            }

            return _store.ContainsBlock(blockHash);
        }

        /// <inheritdoc cref="IStore"/>
        public IEnumerable<KeyValuePair<Address, long>> ListTxNonces(Guid chainId)
        {
            return _store.ListTxNonces(chainId);
        }

        /// <inheritdoc cref="IStore"/>
        public long GetTxNonce(Guid chainId, Address address)
        {
            return _store.GetTxNonce(chainId, address);
        }

        /// <inheritdoc cref="IStore"/>
        public void IncreaseTxNonce(Guid chainId, Address signer, long delta = 1)
        {
            _store.IncreaseTxNonce(chainId, signer, delta);
        }

        /// <inheritdoc cref="IStore"/>
        public bool ContainsTransaction(TxId txId)
        {
            return _store.ContainsTransaction(txId);
        }

        /// <inheritdoc cref="IStore"/>
        public long CountTransactions()
        {
            return _store.CountTransactions();
        }

        /// <inheritdoc cref="IStore"/>
        public long CountBlocks()
        {
            return _store.CountBlocks();
        }

        public void ForkTxNonces(Guid sourceChainId, Guid destinationChainId)
        {
            _store.ForkTxNonces(sourceChainId, destinationChainId);
        }

        /// <inheritdoc cref="IStore"/>
        public void PutBlock<T>(Block<T> block)
            where T : IAction, new()
        {
            if (_blockCache.ContainsKey(block.Hash))
            {
                return;
            }

            _store.PutBlock(block);
            foreach (var tx in block.Transactions)
            {
                PutTransaction(tx);
                StoreTxReferences(tx.Id, block.Hash, block.Index);
            }

            _blockCache.AddOrUpdate(block.Hash, block.ToBlockDigest());
        }

        /// <inheritdoc cref="IStore"/>
        public IEnumerable<Guid> ListChainIds()
        {
            return _store.ListChainIds();
        }

        /// <inheritdoc cref="IStore"/>
        public void DeleteChainId(Guid chainId)
        {
            _store.DeleteChainId(chainId);
        }

        /// <inheritdoc cref="IStore"/>
        public Guid? GetCanonicalChainId()
        {
            return _store.GetCanonicalChainId();
        }

        /// <inheritdoc cref="IStore"/>
        public void SetCanonicalChainId(Guid chainId)
        {
            _store.SetCanonicalChainId(chainId);
        }

        /// <inheritdoc cref="IStore"/>
        public long CountIndex(Guid chainId)
        {
            return _store.CountIndex(chainId);
        }

        /// <inheritdoc cref="IStore"/>
        public IEnumerable<HashDigest<SHA256>> IterateIndexes(
            Guid chainId,
            int offset = 0,
            int? limit = null)
        {
            return _store.IterateIndexes(chainId, offset, limit);
        }

        /// <inheritdoc cref="IStore"/>
        public HashDigest<SHA256>? IndexBlockHash(Guid chainId, long index)
        {
            return _store.IndexBlockHash(chainId, index);
        }

        /// <inheritdoc cref="IStore"/>
        public long AppendIndex(Guid chainId, HashDigest<SHA256> hash)
        {
            return _store.AppendIndex(chainId, hash);
        }

        /// <inheritdoc cref="IStore"/>
        public void ForkBlockIndexes(
            Guid sourceChainId,
            Guid destinationChainId,
            HashDigest<SHA256> branchPoint)
        {
            _store.ForkBlockIndexes(sourceChainId, destinationChainId, branchPoint);
        }

        /// <inheritdoc cref="IStore"/>
        public void StageTransactionIds(IImmutableSet<TxId> txids)
        {
            _store.StageTransactionIds(txids);
        }

        /// <inheritdoc cref="IStore"/>
        public void UnstageTransactionIds(ISet<TxId> txids)
        {
            _store.UnstageTransactionIds(txids);
        }

        /// <inheritdoc cref="IStore"/>
        public IEnumerable<TxId> IterateStagedTransactionIds()
        {
            return _store.IterateStagedTransactionIds();
        }

        /// <inheritdoc cref="IStore"/>
        public IEnumerable<TxId> IterateTransactionIds()
        {
            return _store.IterateTransactionIds();
        }

        /// <inheritdoc cref="IStore"/>
        public Transaction<T> GetTransaction<T>(TxId txid)
            where T : IAction, new()
        {
            return _store.GetTransaction<T>(txid);
        }

        /// <inheritdoc cref="IStore"/>
        public bool DeleteTransaction(TxId txid)
        {
            return _store.DeleteTransaction(txid);
        }

        /// <inheritdoc cref="IStore"/>
        public IEnumerable<HashDigest<SHA256>> IterateBlockHashes()
        {
            return _store.IterateBlockHashes();
        }

        /// <inheritdoc cref="IStore"/>
        public Block<T> GetBlock<T>(HashDigest<SHA256> blockHash)
            where T : IAction, new()
        {
            return _store.GetBlock<T>(blockHash);
        }

        public void PutTransaction<T>(Transaction<T> tx)
            where T : IAction, new()
        {
            _store.PutTransaction(tx);
            StoreSignerReferences(tx.Id, tx.Nonce, tx.Signer);
            StoreUpdatedAddressReferences(tx);
        }

        public void SetBlockPerceivedTime(
            HashDigest<SHA256> blockHash,
            DateTimeOffset perceivedTime)
        {
            _store.SetBlockPerceivedTime(blockHash, perceivedTime);
        }

        public void StoreTxReferences(TxId txId, HashDigest<SHA256> blockHash, long blockIndex)
        {
            var collection = TxRefCollection();
            collection.Upsert(
                new TxRefDoc
                {
                    TxId = txId, BlockHash = blockHash, BlockIndex = blockIndex,
                });
            collection.EnsureIndex(nameof(TxRefDoc.TxId));
            collection.EnsureIndex(nameof(TxRefDoc.BlockIndex));
        }

        public IEnumerable<ValueTuple<TxId, HashDigest<SHA256>>> IterateTxReferences(
            TxId? txId = null,
            bool desc = false,
            int offset = 0,
            int limit = int.MaxValue)
        {
            var collection = TxRefCollection();
            var order = desc ? Query.Descending : Query.Ascending;
            var query = Query.All(nameof(TxRefDoc.BlockIndex), order);

            if (!(txId is null))
            {
                query = Query.And(
                    query,
                    Query.EQ(nameof(TxRefDoc.TxId), txId?.ToByteArray())
                );
            }

            return collection.Find(query, offset, limit).Select(doc => (doc.TxId, doc.BlockHash));
        }

        public void StoreSignerReferences(TxId txId, long txNonce, Address signer)
        {
            var collection = SignerRefCollection();
            collection.Upsert(new AddressRefDoc
            {
                Address = signer, TxNonce = txNonce, TxId = txId,
            });
            collection.EnsureIndex(nameof(AddressRefDoc.AddressString));
            collection.EnsureIndex(nameof(AddressRefDoc.TxNonce));
        }

        public IEnumerable<TxId> IterateSignerReferences(
            Address signer,
            bool desc,
            int offset = 0,
            int limit = int.MaxValue)
        {
            var collection = SignerRefCollection();
            var order = desc ? Query.Descending : Query.Ascending;
            var addressString = signer.ToHex().ToLowerInvariant();
            var query = Query.And(
                Query.All(nameof(AddressRefDoc.TxNonce), order),
                Query.EQ(nameof(AddressRefDoc.AddressString), addressString)
            );
            return collection.Find(query, offset, limit).Select(doc => doc.TxId);
        }

        public void StoreUpdatedAddressReferences<T>(Transaction<T> tx)
            where T : IAction, new()
        {
            foreach (Address address in tx.UpdatedAddresses)
            {
                var collection = UpdatedAddressRefCollection();
                collection.Upsert(new AddressRefDoc
                {
                    Address = address, TxNonce = tx.Nonce, TxId = tx.Id,
                });
                collection.EnsureIndex(nameof(AddressRefDoc.AddressString));
                collection.EnsureIndex(nameof(AddressRefDoc.TxNonce));
            }
        }

        public IEnumerable<TxId> IterateUpdatedAddressReferences(
            Address updatedAddress,
            bool desc,
            int offset = 0,
            int limit = int.MaxValue)
        {
            var collection = UpdatedAddressRefCollection();
            var order = desc ? Query.Descending : Query.Ascending;
            var addressString = updatedAddress.ToHex().ToLowerInvariant();
            var query = Query.And(
                Query.All(nameof(AddressRefDoc.TxNonce), order),
                Query.EQ(nameof(AddressRefDoc.AddressString), addressString)
            );
            return collection.Find(query, offset, limit).Select(doc => doc.TxId);
        }

        private LiteCollection<TxRefDoc> TxRefCollection() =>
            _db.GetCollection<TxRefDoc>(TxRefCollectionName);

        private LiteCollection<AddressRefDoc> SignerRefCollection() =>
            _db.GetCollection<AddressRefDoc>(SignerRefCollectionName);

        private LiteCollection<AddressRefDoc> UpdatedAddressRefCollection() =>
            _db.GetCollection<AddressRefDoc>(UpdatedAddressRefCollectionName);
    }
}
