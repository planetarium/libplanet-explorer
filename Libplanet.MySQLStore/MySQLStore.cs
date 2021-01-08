using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
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

namespace Libplanet.MySqlStore
{
    /// <summary>
    /// The <a href="https://www.mysql.com/">MySql</a> <see cref="IStore"/> implementation.
    /// This stores data in MySql.
    /// </summary>
    /// <seealso cref="IStore"/>
    public class MySqlStore : BaseStore
    {
        private const string BlockDbName = "Block";
        private const string TxDbName = "Transaction";
        private const string UpdatedAddressDbName = "Updated_Address";
        private const string StagedTxDbName = "Staged_Transaction";
        private const string ChainDbName = "Chain";
        private const string CanonChainDbName = "Canon_Chain";
        private static readonly byte[] IndexKeyPrefix = { (byte)'I' };
        private static readonly byte[] BlockKeyPrefix = { (byte)'B' };
        private static readonly byte[] TxKeyPrefix = { (byte)'T' };
        private static readonly byte[] TxNonceKeyPrefix = { (byte)'N' };
        private static readonly byte[] StagedTxKeyPrefix = { (byte)'t' };
        private static readonly byte[] IndexCountKey = { (byte)'c' };
        private static readonly byte[] CanonicalChainIdIdKey = { (byte)'C' };

        private static readonly byte[] EmptyBytes = new byte[0];

        private readonly ILogger _logger;

        private readonly LruCache<TxId, object> _txCache;
        private readonly LruCache<HashDigest<SHA256>, BlockDigest> _blockCache;

        private readonly MySqlCompiler _compiler;
        private readonly string _connectionString;

        /// <summary>
        /// Creates a new <seealso cref="MySqlStore"/>.
        /// </summary>
        /// <param name="options">The options for creating connection string to MySql.</param>
        /// <param name="blockCacheSize">The capacity of the block cache.</param>
        /// <param name="txCacheSize">The capacity of the transaction cache.</param>
        public MySqlStore(
            MySqlStoreOptions options,
            int blockCacheSize = 512,
            int txCacheSize = 1024
        )
        {
            var builder = new MySqlConnector.MySqlConnectionStringBuilder
            {
                Database = options.Database,
                UserID = options.Username,
                Password = options.Password,
                Server = options.Server,
                Port = options.Port,
            };

            _connectionString = builder.ConnectionString;
            _compiler = new MySqlCompiler();

            _logger = Log.ForContext<MySqlStore>();
            _txCache = new LruCache<TxId, object>(capacity: txCacheSize);
            _blockCache = new LruCache<HashDigest<SHA256>, BlockDigest>(capacity: blockCacheSize);
        }

        /// <inheritdoc/>
        public override IEnumerable<Guid> ListChainIds()
        {
            IList<ChainModel> chain = SelectAll<ChainModel>(ChainDbName, "Cf");

            if (chain.Any())
            {
                foreach (ChainModel c in chain)
                {
                    Guid guid;
                    try
                    {
                        guid = Guid.Parse(c.Cf);
                    }
                    catch (FormatException)
                    {
                        continue;
                    }

                    yield return guid;
                }
            }
        }

        /// <inheritdoc/>
        public override void DeleteChainId(Guid chainId)
        {
            _logger.Debug($"Deleting chainID: {chainId}.");

            try
            {
                Delete(ChainDbName, "Cf", chainId.ToString());
            }
            catch (MySqlException e)
            {
                _logger.Debug($"MySql error code: {e}.", e);
                _logger.Debug($"No such chain ID in _chainDb: {chainId}.", chainId);
            }
        }

        /// <inheritdoc />
        public override Guid? GetCanonicalChainId()
        {
            var chain = Select<ChainModel, byte[]>(CanonChainDbName, "Key", CanonicalChainIdIdKey);

            if (!chain.Any())
            {
                return (Guid?)null;
            }
            else
            {
                Guid guid;
                guid = new Guid(chain[0].Value);
                return guid;
            }
        }

        /// <inheritdoc />
        public override void SetCanonicalChainId(Guid chainId)
        {
            byte[] bytes = chainId.ToByteArray();

            Insert(
                CanonChainDbName,
                new Dictionary<string, object>
                {
                    ["Key"] = CanonicalChainIdIdKey,
                    ["Value"] = bytes,
                    ["Cf"] = chainId.ToString(),
                    ["Prefix"] = CanonicalChainIdIdKey,
                },
                "Key",
                CanonicalChainIdIdKey);
        }

        /// <inheritdoc/>
        public override long CountIndex(Guid chainId)
        {
            var index = Select<ChainModel, byte[]>(
                ChainDbName,
                "Key",
                IndexCountKey);

            byte[] bytes = null;

            if (index.Any())
            {
                foreach (var i in index)
                {
                    if (i.Cf == chainId.ToString())
                    {
                        bytes = i.Value;
                    }
                }
            }

            return bytes is null
                ? 0
                : MySqlStoreBitConverter.ToInt64(bytes);
        }

        /// <inheritdoc/>
        public override IEnumerable<HashDigest<SHA256>> IterateIndexes(
            Guid chainId,
            int offset,
            int? limit)
        {
            int count = 0;
            byte[] prefix = IndexKeyPrefix;

            using QueryFactory db = MySqlUtils.OpenMySqlDB(_connectionString, _compiler);
            Query query = db.Query(ChainDbName).Select().Where("Prefix", prefix);
            query = query.Offset(offset);
            IEnumerable<ChainModel> chains = query.Where("Cf", chainId.ToString()).Get<ChainModel>();

            foreach (var chain in chains)
            {
                if (count >= limit)
                {
                    break;
                }

                byte[] value = chain.Value;
                yield return new HashDigest<SHA256>(value);

                count += 1;
            }
        }

        /// <inheritdoc/>
        public override HashDigest<SHA256>? IndexBlockHash(Guid chainId, long index)
        {
            if (index < 0)
            {
                index += CountIndex(chainId);

                if (index < 0)
                {
                    return null;
                }
            }

            byte[] indexBytes = MySqlStoreBitConverter.GetBytes(index);

            byte[] key = IndexKeyPrefix.Concat(indexBytes).ToArray();

            var indices = Select<ChainModel, byte[]>(
                ChainDbName,
                "Key",
                key);

            byte[] bytes = null;
            if (indices.Any())
            {
                foreach (var idx in indices)
                {
                    if (idx.Cf == chainId.ToString())
                    {
                        bytes = idx.Value;
                    }
                }
            }

            return bytes is null
                ? (HashDigest<SHA256>?)null
                : new HashDigest<SHA256>(bytes);
        }

        /// <inheritdoc/>
        public override long AppendIndex(Guid chainId, HashDigest<SHA256> hash)
        {
            long index = CountIndex(chainId);
            byte[] indexBytes = MySqlStoreBitConverter.GetBytes(index);
            byte[] key = IndexKeyPrefix.Concat(indexBytes).ToArray();

            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);

            db.Statement(
                "INSERT INTO Chain (`key`, `value`, `cf`, `prefix`) VALUES " +
                "(@key1, @value1, @cf1, @prefix1), (@key2, @value2, @cf2, @prefix2) ON DUPLICATE KEY UPDATE " +
                "`key` = VALUES(`key`), `value` = VALUES(`value`), `cf` = VALUES(`cf`), `prefix` = VALUES(`prefix`)",
                new
                {
                    key1 = key,
                    value1 = hash.ToByteArray(),
                    cf1 = chainId.ToString(),
                    prefix1 = IndexKeyPrefix,
                    key2 = IndexCountKey,
                    value2 = MySqlStoreBitConverter.GetBytes(index + 1),
                    cf2 = chainId.ToString(),
                    prefix2 = IndexCountKey,
                });

            return index;
        }

        /// <inheritdoc/>
        public override void ForkBlockIndexes(
            Guid sourceChainId,
            Guid destinationChainId,
            HashDigest<SHA256> branchPoint)
        {
            HashDigest<SHA256>? genesisHash = IterateIndexes(sourceChainId, 0, 1)
                .Cast<HashDigest<SHA256>?>()
                .FirstOrDefault();

            if (genesisHash is null || branchPoint.Equals(genesisHash))
            {
                return;
            }

            foreach (HashDigest<SHA256> hash in IterateIndexes(sourceChainId, 1, null))
            {
                AppendIndex(destinationChainId, hash);

                if (hash.Equals(branchPoint))
                {
                    break;
                }
            }
        }

        /// <inheritdoc/>
        public override void StageTransactionIds(IImmutableSet<TxId> txids)
        {
            foreach (TxId txId in txids)
            {
                byte[] key = StagedTxKey(txId);

                Insert(
                    StagedTxDbName,
                    new Dictionary<string, object>
                    {
                        ["Key"] = key,
                        ["Value"] = EmptyBytes,
                        ["Prefix"] = StagedTxKeyPrefix,
                    },
                    "Key",
                    key);
            }
        }

        /// <inheritdoc/>
        public override void UnstageTransactionIds(ISet<TxId> txids)
        {
            foreach (TxId txId in txids)
            {
                byte[] key = StagedTxKey(txId);
                Delete(StagedTxDbName, "Key", key);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<TxId> IterateStagedTransactionIds()
        {
            // Do Nothing
            yield return new TxId(new byte[0]);
        }

        /// <inheritdoc/>
        public override IEnumerable<TxId> IterateTransactionIds()
        {
            byte[] prefix = TxKeyPrefix;

            using QueryFactory db = MySqlUtils.OpenMySqlDB(_connectionString, _compiler);
            Query query = db.Query(TxDbName).Select(new[] { "Key", "Value" }).Where("Prefix", prefix);
            IEnumerable<TransactionModel> transactions = query.Get<TransactionModel>();

            foreach (var transaction in transactions)
            {
                byte[] key = transaction.Key;
                byte[] txIdBytes = key.Skip(prefix.Length).ToArray();

                var txId = new TxId(txIdBytes);
                yield return txId;
            }
        }

        /// <inheritdoc/>
        public override Transaction<T> GetTransaction<T>(TxId txid)
        {
            if (_txCache.TryGetValue(txid, out object cachedTx))
            {
                return (Transaction<T>)cachedTx;
            }

            byte[] key = TxKey(txid);
            var transaction = Select<TransactionModel, byte[]>(TxDbName, "Key", key);

            if (!transaction.Any())
            {
                return null;
            }

            byte[] bytes = transaction[0].Value;
            Transaction<T> tx = Transaction<T>.Deserialize(bytes);
            _txCache.AddOrUpdate(txid, tx);
            return tx;
        }

        /// <inheritdoc/>
        public override void PutTransaction<T>(Transaction<T> tx)
        {
            if (_txCache.ContainsKey(tx.Id))
            {
                return;
            }

            byte[] key = TxKey(tx.Id);

            if (Select<TransactionModel, byte[]>(TxDbName, "Key", key).Any())
            {
                return;
            }

            Insert(
                TxDbName,
                new Dictionary<string, object>
                {
                    ["Id"] = tx.Id.ToString(),
                    ["Nonce"] = tx.Nonce.ToString(CultureInfo.CreateSpecificCulture("en-US")),
                    ["Signer"] = tx.Signer.ToString(),
                    ["Signature"] = Convert.ToBase64String(tx.Signature),
                    ["Timestamp"] = tx.Timestamp.ToString(CultureInfo.CreateSpecificCulture("en-US")),
                    ["Public_key"] = ByteUtil.Hex(tx.PublicKey.Format(true)),
                    ["Genesis_hash"] = tx.GenesisHash.ToString(),
                    ["Bytes_length"] = tx.BytesLength.ToString(CultureInfo.CreateSpecificCulture("en-US")),
                    ["Key"] = key,
                    ["Value"] = tx.Serialize(true),
                    ["Prefix"] = TxKeyPrefix,
                },
                "Key",
                key);

            PutUpdatedAddress(tx.UpdatedAddresses, tx.Id);
            _txCache.AddOrUpdate(tx.Id, tx);
        }

        /// <inheritdoc/>
        public override bool DeleteTransaction(TxId txid)
        {
            byte[] key = TxKey(txid);

            if (!Select<TransactionModel, byte[]>(TxDbName, "Key", key).Any())
            {
                return false;
            }

            _txCache.Remove(txid);
            Delete(TxDbName, "Key", key);
            Delete(UpdatedAddressDbName, "Transaction_Id", txid.ToString());

            return true;
        }

        public void PutUpdatedAddress(IImmutableSet<Address> updatedAddresses, TxId txid)
        {
            InsertMany(
                UpdatedAddressDbName,
                new[] { "Address", "Transaction_Id" },
                updatedAddresses.Select(
                    addr => new object[]
                    {
                        addr.ToString(), txid.ToString(),
                    }));
        }

        /// <inheritdoc/>
        public override bool ContainsTransaction(TxId txId)
        {
            if (_txCache.ContainsKey(txId))
            {
                return true;
            }

            byte[] key = TxKey(txId);

            return Select<TransactionModel, byte[]>(TxDbName, "Key", key).Any();
        }

        /// <inheritdoc/>
        public override IEnumerable<HashDigest<SHA256>> IterateBlockHashes()
        {
            byte[] prefix = BlockKeyPrefix;

            IEnumerable<BlockModel> blocks = Select<BlockModel, byte[]>(BlockDbName, "Prefix", prefix);

            foreach (var block in blocks)
            {
                byte[] key = block.Key;
                byte[] hashBytes = key.Skip(prefix.Length).ToArray();

                var blockHash = new HashDigest<SHA256>(hashBytes);
                yield return blockHash;
            }
        }

        /// <inheritdoc/>
        public override BlockDigest? GetBlockDigest(HashDigest<SHA256> blockHash)
        {
            if (_blockCache.TryGetValue(blockHash, out BlockDigest cachedDigest))
            {
                return cachedDigest;
            }

            byte[] key = BlockKey(blockHash);
            var block = Select<BlockModel, byte[]>(BlockDbName, "Key", key);

            if (!block.Any())
            {
                return null;
            }

            byte[] bytes = block[0].Value;
            BlockDigest blockDigest = BlockDigest.Deserialize(bytes);
            _blockCache.AddOrUpdate(blockHash, blockDigest);
            return blockDigest;
        }

        /// <inheritdoc/>
        public override void PutBlock<T>(Block<T> block)
        {
            if (_blockCache.ContainsKey(block.Hash))
            {
                return;
            }

            byte[] key = BlockKey(block.Hash);

            if (Select<BlockModel, byte[]>(BlockDbName, "Key", key).Any())
            {
                return;
            }

            foreach (var tx in block.Transactions)
            {
                PutTransaction(tx);
            }

            byte[] value = block.ToBlockDigest().Serialize();

            Insert(
                BlockDbName,
                new Dictionary<string, object>
                {
                    ["Index"] = block.Index,
                    ["Hash"] = block.Hash.ToString(),
                    ["Pre_evaluation_hash"] = block.PreEvaluationHash.ToString(),
                    ["State_root_hash"] = block.StateRootHash.ToString(),
                    ["Difficulty"] = block.Difficulty,
                    ["Total_difficulty"] = (long)block.TotalDifficulty,
                    ["Nonce"] = block.Nonce.ToString(),
                    ["Miner"] = block.Miner.ToString(),
                    ["Previous_hash"] = block.PreviousHash.ToString(),
                    ["Timestamp"] = block.Timestamp.ToString(CultureInfo.CreateSpecificCulture("en-US")),
                    ["Tx_hash"] = block.TxHash.ToString(),
                    ["Bytes_length"] = block.BytesLength.ToString(CultureInfo.CreateSpecificCulture("en-US")),
                    ["Key"] = key,
                    ["Value"] = value,
                    ["Prefix"] = BlockKeyPrefix,
                },
                "Key",
                key);
            _blockCache.AddOrUpdate(block.Hash, block.ToBlockDigest());
        }

        /// <inheritdoc/>
        public override bool DeleteBlock(HashDigest<SHA256> blockHash)
        {
            byte[] key = BlockKey(blockHash);

            if (!Select<BlockModel, byte[]>(BlockDbName, "Key", key).Any())
            {
                return false;
            }

            Delete(BlockDbName, "Key", key);
            _blockCache.Remove(blockHash);

            return true;
        }

        /// <inheritdoc/>
        public override bool ContainsBlock(HashDigest<SHA256> blockHash)
        {
            if (_blockCache.ContainsKey(blockHash))
            {
                return true;
            }

            byte[] key = BlockKey(blockHash);

            return Select<BlockModel, byte[]>(BlockDbName, "Key", key).Any();
        }

        /// <inheritdoc/>
        public override IEnumerable<KeyValuePair<Address, long>> ListTxNonces(Guid chainId)
        {
            byte[] prefix = TxNonceKeyPrefix;

            using QueryFactory db = MySqlUtils.OpenMySqlDB(_connectionString, _compiler);
            Query query = db.Query(ChainDbName).Select().Where("Prefix", prefix);
            IEnumerable<ChainModel> txNonces = query.Where("Cf", chainId.ToString()).Get<ChainModel>();

            foreach (var txNonce in txNonces)
            {
                byte[] addressBytes = txNonce.Key.Skip(prefix.Length).ToArray();
                Address address = new Address(addressBytes);
                long nonce = MySqlStoreBitConverter.ToInt64(txNonce.Value);
                yield return new KeyValuePair<Address, long>(address, nonce);
            }
        }

        /// <inheritdoc/>
        public override long GetTxNonce(Guid chainId, Address address)
        {
            byte[] key = TxNonceKey(address);
            IList<ChainModel> chain = Select<ChainModel, byte[]>(ChainDbName, "Key", key);

            byte[] bytes = null;

            if (chain.Any())
            {
                foreach (var c in chain)
                {
                    if (c.Cf == chainId.ToString())
                    {
                        bytes = c.Value;
                    }
                }
            }

            return bytes is null
                ? 0
                : MySqlStoreBitConverter.ToInt64(bytes);
        }

        /// <inheritdoc/>
        public override void IncreaseTxNonce(Guid chainId, Address signer, long delta = 1)
        {
            long nextNonce = GetTxNonce(chainId, signer) + delta;

            byte[] key = TxNonceKey(signer);
            byte[] bytes = MySqlStoreBitConverter.GetBytes(nextNonce);

            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);

            db.Statement(
                "INSERT INTO Chain (`key`, `value`, `cf`, `prefix`) VALUES (@key, @value, @cf, @prefix) " +
                "ON DUPLICATE KEY UPDATE `key` = VALUES(`key`), `value` = VALUES(`value`), `cf` = VALUES(`cf`), `prefix` = VALUES(`prefix`)",
                new
                {
                    key = key,
                    value = bytes,
                    cf = chainId.ToString(),
                    prefix = TxNonceKeyPrefix,
                });
        }

        /// <inheritdoc/>
        public override long CountTransactions()
        {
            return IterateTransactionIds().LongCount();
        }

        /// <inheritdoc/>
        public override long CountBlocks()
        {
            return IterateBlockHashes().LongCount();
        }

        public override void Dispose()
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);

            db.Statement($"SELECT Concat('TRUNCATE TABLE ', TABLE_NAME)" +
                "FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'libplanet.mysql'");
        }

        private byte[] BlockKey(HashDigest<SHA256> blockHash)
        {
            return BlockKeyPrefix.Concat(blockHash.ToByteArray()).ToArray();
        }

        private byte[] TxKey(TxId txId)
        {
            return TxKeyPrefix.Concat(txId.ToByteArray()).ToArray();
        }

        private byte[] TxNonceKey(Address address)
        {
            return TxNonceKeyPrefix
                .Concat(address.ToByteArray())
                .ToArray();
        }

        private byte[] StagedTxKey(TxId txId)
        {
            return StagedTxKeyPrefix.Concat(txId.ToByteArray()).ToArray();
        }

        #nullable enable
        private IList<T> Select<T, U>(
            string tableName,
            string? column,
            U id)
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
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

        private IList<T> SelectAll<T>(string tableName, string column)
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
            try
            {
                var rows = db.Query(tableName).WhereNotNull(column).Get<T>();
                return rows.ToList();
            }
            catch (MySqlException e)
            {
                Log.Debug(e.ErrorCode.ToString());
                throw;
            }
        }

        private void Insert<T>(string tableName, IReadOnlyDictionary<string, object> data, string? key, T value)
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
            try
            {
                db.Query(tableName).Insert(data);
            }
            catch (MySqlException e)
            {
                if (e.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    Log.Debug($"Update DuplicateKeyEntry in {tableName})");
                    if (key != null && value != null)
                    {
                        Update(tableName, data, key, value);
                    }
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
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
            try
            {
                db.Query(tableName).Insert(columns, data);
            }
            catch (MySqlException e)
            {
                if (e.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    Log.Debug($"Update DuplicateKeyEntry in {tableName})");
                    foreach (var d in data)
                    {
                        var updateData = new Dictionary<string, object>();
                        for (var i = 0; i < columns.Count(); i++)
                        {
                            updateData.Add(columns[i], d[i]);
                        }

                        Update(tableName, updateData, columns[0], d[0]);
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        private void Update<T>(string tableName, IReadOnlyDictionary<string, object> data, string key, T value)
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
            try
            {
                db.Query(tableName).Where(key, value).Update(data);
            }
            catch (MySqlException e)
            {
                if (e.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    Log.Debug($"Ignore DuplicateKeyEntry in {tableName})");
                }
                else
                {
                    throw;
                }
            }
        }

        private void Delete<T>(string tableName, string column, T id)
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
            try
            {
                db.Query(tableName).Where(column, id).Delete();
            }
            catch (MySqlException e)
            {
                Log.Debug(e.ErrorCode.ToString());
                throw;
            }
        }
    }
}
