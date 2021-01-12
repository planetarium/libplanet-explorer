#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Libplanet.Store.Trie;
using MySqlConnector;
using MySqlStore.Models;
using Serilog;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Libplanet.MySqlStore
{
    /// <summary>
    /// The <a href="https://www.mysql.com/">MySql</a> <see cref="IStore"/> implementation.
    /// This stores data in MySql.
    /// </summary>
    public class MySqlKeyValueStore : IKeyValueStore, IDisposable
    {
        private readonly QueryFactory? _keyValueDb;
        private readonly MySqlCompiler _compiler;
        private readonly string _connectionString;
        private readonly ILogger _logger;
        private readonly string _dbName;

        /// <summary>
        /// Creates a new <see cref="MySqlKeyValueStore"/>.
        /// </summary>
        /// <param name="options">The options for creating connection string to MySql.</param>
        /// <param name="dbName">The name of table in MySql.</param>
        public MySqlKeyValueStore(MySqlStoreOptions options, string dbName)
        {
            var builder = new MySqlConnectionStringBuilder
            {
                Database = options.Database,
                UserID = options.Username,
                Password = options.Password,
                Server = options.Server,
                Port = options.Port,
            };

            _connectionString = builder.ConnectionString;
            _compiler = new MySqlCompiler();
            _logger = Log.ForContext<MySqlKeyValueStore>();
            _dbName = dbName;
            _keyValueDb = MySqlUtils.OpenMySqlDB(_connectionString, _compiler);
        }

        /// <inheritdoc/>
        public byte[] Get(byte[] key)
        {
            var value = Select<KeyValueModel>(_dbName, "Key", key);

            if (!value.Any())
            {
                throw new KeyNotFoundException(
                    $"There were no elements that correspond to the key (hex: {ByteUtil.Hex(key)}).");
            }
            else
            {
                return value[0].Value;
            }
        }

        /// <inheritdoc/>
        public void Set(byte[] key, byte[] value)
        {
            Insert(
                _dbName,
                new Dictionary<string, object>
                {
                    ["Key"] = key,
                    ["Value"] = value,
                },
                "Key",
                key);
        }

        /// <inheritdoc/>
        public void Delete(byte[] key)
        {
            Delete(_dbName, "Key", key);
        }

        public void Dispose()
        {
            QueryFactory db = new QueryFactory(new MySqlConnection(_connectionString), _compiler);
            db.Statement($"truncate table {_dbName}");
        }

        /// <inheritdoc/>
        public bool Exists(byte[] key)
        {
            var values = Select<KeyValueModel>(_dbName, "Key", key);

            if (values is null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        /// <inheritdoc/>
        public IEnumerable<byte[]> ListKeys()
        {
            var keys = SelectAll<KeyValueModel>(_dbName, "Key");

            if (keys is null)
            {
                byte[] emptyBytes = new byte[0];
                yield return emptyBytes;
            }
            else
            {
                foreach (var key in keys)
                {
                    yield return key.Key;
                }
            }
        }

        private IList<T> Select<T>(
            string tableName,
            string? column,
            byte[] id)
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
