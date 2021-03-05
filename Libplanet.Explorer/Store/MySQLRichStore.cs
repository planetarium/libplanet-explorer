using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Bencodex.Types;
using Libplanet.Action;
using Libplanet.Blocks;
using Libplanet.Store;
using Libplanet.Tx;
using LruCacheNet;
using MySqlConnector;
using MySqlStore.Models;
using Serilog;
using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Libplanet.Explorer.Store
{
    // It assumes running Explorer as online-mode.
    public class MySQLRichStore : IRichStore
    {
        private const string BlockDbName = "block";
        private const string TxDbName = "transaction";
        private const string TxRefDbName = "tx_references";
        private const string SignerRefDbName = "signer_references";
        private const string UpdatedAddressRefDbName = "updated_address_references";

        private readonly LruCache<HashDigest<SHA256>, BlockDigest> _blockCache;

        // FIXME we should separate it.
        private readonly IStore _store;

        private readonly MySqlCompiler _compiler;
        private readonly string _connectionString;

        private string _filePath;

        public MySQLRichStore(IStore store, MySQLRichStoreOptions options)
        {
            _store = store;

            var builder = new MySqlConnectionStringBuilder
            {
                Database = options.Database,
                UserID = options.Username,
                Password = options.Password,
                Server = options.Server,
                Port = options.Port,
                AllowLoadLocalInfile = true,
            };

            _connectionString = builder.ConnectionString;
            _compiler = new MySqlCompiler();

            _blockCache = new LruCache<HashDigest<SHA256>, BlockDigest>(capacity: 512);
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
            if (!Select<BlockModel>(BlockDbName, "hash", blockHash.ToByteArray()).Any())
            {
                return false;
            }

            Delete(BlockDbName, "hash", blockHash.ToByteArray());
            _blockCache.Remove(blockHash);

            _store.DeleteBlock(blockHash);
            return true;
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

        /// <inheritdoc cref="IStore"/>
        public void PutBlock<T>(Block<T> block)
            where T : IAction, new()
        {
            _store.PutBlock(block);
            foreach (var tx in block.Transactions)
            {
                PutTransaction(tx);
                StoreTxReferences(tx.Id, block.Hash, tx.Nonce);
            }

            Insert(
                BlockDbName,
                new Dictionary<string, object>
                {
                    ["index"] = block.Index,
                    ["hash"] = block.Hash.ToByteArray(),
                    ["pre_evaluation_hash"] = block.PreEvaluationHash.ToByteArray(),
                    ["state_root_hash"] = block.StateRootHash?.ToByteArray(),
                    ["difficulty"] = block.Difficulty,
                    ["total_difficulty"] = (long)block.TotalDifficulty,
                    ["nonce"] = block.Nonce.ToByteArray(),
                    ["miner"] = block.Miner?.ToByteArray(),
                    ["previous_hash"] = block.PreviousHash?.ToByteArray(),
                    ["timestamp"] = block.Timestamp.ToString(),
                    ["tx_hash"] = block.TxHash?.ToByteArray(),
                    ["bytes_length"] = block.BytesLength,
                },
                "index",
                block.Index);
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
            if (!Select<TransactionModel>(TxDbName, "tx_id", txid.ToByteArray()).Any())
            {
                return false;
            }

            Delete(TxDbName, "tx_id", txid.ToByteArray());
            Delete(UpdatedAddressRefDbName, "tx_id", txid.ToByteArray());

            _store.DeleteTransaction(txid);
            return true;
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
            // bulk load data into `updated_address_references`
            _filePath = Path.GetTempFileName();

            MySqlConnection conn = new MySqlConnection(_connectionString);
            StreamWriter bulkFile = new StreamWriter(_filePath);

            foreach (Address addr in tx.UpdatedAddresses)
            {
                bulkFile.WriteLine(
                    $"{ByteUtil.Hex(addr.ToByteArray())}," +
                    $"{ByteUtil.Hex(tx.Id.ToByteArray())}," +
                    $"{tx.Nonce}");
            }

            bulkFile.Flush();
            bulkFile.Close();

            MySqlBulkLoader loader = new MySqlBulkLoader(conn)
            {
                TableName = UpdatedAddressRefDbName,
                FileName = _filePath,
                Timeout = 0,
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                FieldQuotationOptional = true,
                Local = true,
            };

            int count = loader.Load();

            // insert data into `transaction`
            Insert(
                TxDbName,
                new Dictionary<string, object>
                {
                    ["tx_id"] = tx.Id.ToByteArray(),
                    ["nonce"] = tx.Nonce,
                    ["signer"] = tx.Signer.ToByteArray(),
                    ["signature"] = tx.Signature,
                    ["timestamp"] = tx.Timestamp.ToString(),
                    ["public_key"] = ByteUtil.Hex(tx.PublicKey.Format(true)),
                    ["genesis_hash"] = tx.GenesisHash?.ToByteArray(),
                    ["bytes_length"] = tx.BytesLength,
                },
                "tx_id",
                tx.Id.ToByteArray());

            _store.PutTransaction(tx);
            StoreSignerReferences(tx.Id, tx.Nonce, tx.Signer);
        }

        public void SetBlockPerceivedTime(
            HashDigest<SHA256> blockHash,
            DateTimeOffset perceivedTime)
        {
            _store.SetBlockPerceivedTime(blockHash, perceivedTime);
        }

        public void StoreTxReferences(TxId txId, HashDigest<SHA256> blockHash, long txNonce)
        {
           Insert(
                TxRefDbName,
                new Dictionary<string, object>
                {
                    ["tx_id"] = txId.ToByteArray(),
                    ["tx_nonce"] = txNonce,
                    ["block_hash"] = blockHash.ToByteArray(),
                },
                "tx_id",
                txId.ToByteArray());
        }

        public IEnumerable<ValueTuple<TxId, HashDigest<SHA256>>> IterateTxReferences(
            TxId? txId = null,
            bool desc = false,
            int offset = 0,
            int limit = int.MaxValue)
        {
            using QueryFactory db = OpenDB();
            Query query = db.Query(TxRefDbName).Select(new[] { "tx_id", "block_hash" });
            if (!(txId is null))
            {
                query = query.Where("tx_id", txId?.ToByteArray());
            }

            query = desc ? query.OrderByDesc("tx_nonce") : query.OrderBy("tx_nonce");
            query = query.Offset(offset).Limit(limit);
            return db.GetDictionary(query).Select(dict => new ValueTuple<TxId, HashDigest<SHA256>>(
                new TxId((byte[])dict["tx_id"]),
                new HashDigest<SHA256>((byte[])dict["block_hash"])));
        }

        public void StoreSignerReferences(TxId txId, long txNonce, Address signer)
        {
            Insert(
                SignerRefDbName,
                new Dictionary<string, object>
                {
                    ["signer"] = signer.ToByteArray(),
                    ["tx_id"] = txId.ToByteArray(),
                    ["tx_nonce"] = txNonce,
                },
                "tx_id",
                txId.ToByteArray());
        }

        public IEnumerable<TxId> IterateSignerReferences(
            Address signer,
            bool desc,
            int offset = 0,
            int limit = int.MaxValue)
        {
            using QueryFactory db = OpenDB();
            var query = db.Query(SignerRefDbName).Where("signer", signer.ToByteArray())
                .Offset(offset)
                .Limit(limit)
                .Select("tx_id");
            query = desc ? query.OrderByDesc("tx_nonce") : query.OrderBy("tx_nonce");
            return query.OrderBy("tx_nonce")
                .Get<byte[]>()
                .Select(bytes => new TxId(bytes));
        }

        public void StoreUpdatedAddressReferences(
            TxId txId,
            long txNonce,
            Address updatedAddress)
        {
            Insert(
                UpdatedAddressRefDbName,
                new Dictionary<string, object>
                {
                    ["updated_address"] = updatedAddress.ToByteArray(),
                    ["tx_id"] = txId.ToByteArray(),
                    ["tx_nonce"] = txNonce,
                },
                "tx_id",
                txId.ToByteArray());
        }

        public IEnumerable<TxId> IterateUpdatedAddressReferences(
            Address updatedAddress,
            bool desc,
            int offset = 0,
            int limit = int.MaxValue)
        {
            using QueryFactory db = OpenDB();
            var query = db.Query(UpdatedAddressRefDbName)
                .Where("updated_address", updatedAddress.ToByteArray())
                .Offset(offset)
                .Limit(limit)
                .Select("tx_id");
            query = desc ? query.OrderByDesc("tx_nonce") : query.OrderBy("tx_nonce");
            return query.OrderBy("tx_nonce")
                .Get<byte[]>()
                .Select(bytes => new TxId(bytes));
        }

        private QueryFactory OpenDB() =>
            new QueryFactory(new MySqlConnection(_connectionString), _compiler);

        private IList<T> Select<T>(
            string tableName,
            string column,
            byte[] id)
        {
            using QueryFactory db = OpenDB();
            try
            {
                var rows = db.Query(tableName).Where(column, id).Get<T>();
                return rows.ToList();
            }
            catch (MySqlException e)
            {
                Log.Debug(e.ErrorCode.ToString());
                throw;
            }
        }

        private void Insert<T>(
            string tableName,
            IReadOnlyDictionary<string, object> data,
            string key,
            T value)
        {
            using QueryFactory db = OpenDB();
            try
            {
                db.Query(tableName).Insert(data);
            }
            catch (MySqlException e)
            {
                if (e.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    if (key != null && value != null)
                    {
                        Update(tableName, data, key, value);
                    }

                    Log.Debug($"Update DuplicateKeyEntry in {tableName}");
                }
                else
                {
                    throw;
                }
            }
        }

        private void InsertMany(
            string tableName,
            string[] columns,
            IEnumerable<object[]> data)
        {
            using QueryFactory db = OpenDB();
            try
            {
                db.Query(tableName).Insert(columns, data);
            }
            catch (MySqlException e)
            {
                if (e.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    Log.Debug("Ignore DuplicateKeyEntry");
                }
                else
                {
                    throw;
                }
            }
        }

        private void Update<T>(
            string tableName,
            IReadOnlyDictionary<string, object> data,
            string key,
            T value)
        {
            using QueryFactory db = OpenDB();
            try
            {
                db.Query(tableName).Where(key, value).Update(data);
            }
            catch (MySqlException e)
            {
                if (e.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    Log.Debug($"Ignore DuplicateKeyEntry in {tableName}");
                }
                else
                {
                    throw;
                }
            }
        }

        private void Delete<T>(string tableName, string column, T id)
        {
            using QueryFactory db = OpenDB();
            try
            {
                db.Query(tableName).Where(column, id).Delete();
            }
            catch (MySqlException e)
            {
                Log.Debug(e.Message);
                throw;
            }
        }
    }
}
