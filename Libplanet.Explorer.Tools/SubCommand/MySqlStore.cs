using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using Bencodex.Types;
using System.Threading.Tasks;
using Cocona;
using System.Security.Cryptography;
using Libplanet.Action;
using Libplanet.RocksDBStore;
using Libplanet.Store;
using Libplanet.Tx;
using MySqlConnector;

namespace Libplanet.Explorer.Tools.SubCommand
{
    public class MySqlStore
    {
        private const string BlockDbName = "block";
        private const string TxDbName = "transaction";
        private const string TxRefDbName = "tx_references";
        private const string SignerRefDbName = "signer_references";
        private const string UpdatedAddressRefDbName = "updated_address_references";

        [Command(Description = "Migrate rocksdb store to mysql csv file.")]
        public void Migration(
            string storePath,
            string outputDirectory,
            [Option(
                "rocksdb-storetype",
                Description = "Store type of RocksDb to migrate.")]
            string rocksdbStoreType
            )
        {
            IStore originStore;

            if (rocksdbStoreType == "new")
            {
                originStore = new RocksDBStore.RocksDBStore(
                    storePath,
                    dbConnectionCacheSize: 10000);
            }
            else if (rocksdbStoreType == "mono")
            {
                originStore = new RocksDBStore.MonoRocksDBStore(storePath);
            }
            else
            {
                throw new CommandExitedException("Invalid rocksdb-storetype. Please enter 'new' or 'mono'", -1);
            }

            long totalLength = originStore.CountBlocks();

            if (totalLength == 0)
            {
                throw new CommandExitedException("Invalid rocksdb-store. Please enter a valid store path", -1);
            }

            if (!Directory.Exists(outputDirectory))
            {
                throw new CommandExitedException("Invalid outputDirectory. Please enter a valid output path", -1);
            }

            string blockFilePath = outputDirectory + $"/{BlockDbName}.csv";
            StreamWriter blockBulkFile = new StreamWriter(blockFilePath);

            string txFilePath = outputDirectory + $"/{TxDbName}.csv";
            StreamWriter txBulkFile = new StreamWriter(txFilePath);

            string txRefFilePath = outputDirectory + $"/{TxRefDbName}.csv";
            StreamWriter txRefBulkFile = new StreamWriter(txRefFilePath);

            string signerRefFilePath = outputDirectory + $"/{SignerRefDbName}.csv";
            StreamWriter signerRefBulkFile = new StreamWriter(signerRefFilePath);

            string updatedAddressRefFilePath = outputDirectory + $"/{UpdatedAddressRefDbName}.csv";
            StreamWriter updatedAddressRefBulkFile = new StreamWriter(
                updatedAddressRefFilePath);

            WriteHeader(BlockDbName, blockBulkFile);
            WriteHeader(TxDbName, txBulkFile);
            WriteHeader(TxRefDbName, txRefBulkFile);
            WriteHeader(SignerRefDbName, signerRefBulkFile);
            WriteHeader(UpdatedAddressRefDbName, updatedAddressRefBulkFile);
            MigrateData(
                originStore,
                totalLength,
                blockBulkFile,
                txBulkFile,
                txRefBulkFile,
                signerRefBulkFile,
                updatedAddressRefBulkFile);
        }

        private static string Hex(byte[] bytes)
        {
            if (bytes == null)
            {
                throw new ArgumentNullException(nameof(bytes));
            }

            string s = BitConverter.ToString(bytes);
            return s.Replace("-", string.Empty).ToLower(CultureInfo.InvariantCulture);
        }

        public void MigrateData(
            IStore originStore,
            long totalLength,
            StreamWriter blockBulkFile,
            StreamWriter txBulkFile,
            StreamWriter txRefBulkFile,
            StreamWriter signerRefBulkFile,
            StreamWriter updatedAddressRefBulkFile
        )
        {
            Console.WriteLine("Start migrating block.");
            Stopwatch sw = new Stopwatch();
            sw.Start();

            // To see the base mysql schema used for this migration tool,
            // please refer to ../sql/initialize-rich-store.sql.
            foreach (var item in
                originStore.IterateBlockHashes().Select((value, i) => new { i, value }))
            {
                Console.WriteLine($"block progress: {item.i}/{totalLength}");
                var block = originStore.GetBlock<DummyAction>(item.value);
                foreach (var tx in block.Transactions)
                {
                    PutTransaction(tx, txBulkFile);
                    StoreTxReferences(tx.Id, block.Hash, tx.Nonce, txRefBulkFile);
                    StoreSignerReferences(tx.Id, tx.Nonce, tx.Signer, signerRefBulkFile);
                    StoreUpdatedAddressReferences(tx, updatedAddressRefBulkFile);
                }

                try
                {
                    blockBulkFile.WriteLine(
                        $"{block.Index};" +
                        $"{block.Hash.ToString()};" +
                        $"{block.PreEvaluationHash.ToString()};" +
                        $"{block.StateRootHash?.ToString()};" +
                        $"{block.Difficulty};" +
                        $"{(long)block.TotalDifficulty};" +
                        $"{block.Nonce.ToString()};" +
                        $"{block.Miner?.ToString()};" +
                        $"{block.PreviousHash?.ToString()};" +
                        $"{block.Timestamp.ToString()};" +
                        $"{block.TxHash?.ToString()};" +
                        $"{block.ProtocolVersion}");
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(e.Message);
                    Console.Error.Flush();
                    throw;
                }
            }

            blockBulkFile.Flush();
            blockBulkFile.Close();
            blockBulkFile.Dispose();

            txBulkFile.Flush();
            txBulkFile.Close();
            txBulkFile.Dispose();

            txRefBulkFile.Flush();
            txRefBulkFile.Close();
            txRefBulkFile.Dispose();

            signerRefBulkFile.Flush();
            signerRefBulkFile.Close();
            signerRefBulkFile.Dispose();

            updatedAddressRefBulkFile.Flush();
            updatedAddressRefBulkFile.Close();
            updatedAddressRefBulkFile.Dispose();

            sw.Stop();
            Console.WriteLine("Finished block data migration.");
            Console.WriteLine("Time elapsed: {0:hh\\:mm\\:ss}", sw.Elapsed);
        }

        private void PutTransaction<T>(Transaction<T> tx, StreamWriter txBulkFile)
            where T : IAction, new()
        {
            try
            {
                txBulkFile.WriteLine(
                    $"{tx.Id.ToString()};" +
                    $"{tx.Nonce};" +
                    $"{tx.Signer.ToString()};" +
                    $"{Hex(tx.Signature)};" +
                    $"{tx.Timestamp.ToString()};" +
                    $"{Hex(tx.PublicKey.Format(true))};" +
                    $"{tx.GenesisHash?.ToString()};" +
                    $"{tx.BytesLength}");
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.Flush();
                throw;
            }
        }

        private void StoreTxReferences(
            TxId txId,
            Libplanet.HashDigest<SHA256> blockHash,
            long txNonce,
            StreamWriter txRefBulkFile)
        {
            try
            {
                txRefBulkFile.WriteLine(
                    $"{txId.ToString()};" +
                    $"{blockHash.ToString()};" +
                    $"{txNonce}");
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.Flush();
                throw;
            }
        }

        private void StoreSignerReferences(
            TxId txId,
            long txNonce,
            Libplanet.Address signer,
            StreamWriter signerRefBulkFile)
        {
            try
            {
                signerRefBulkFile.WriteLine(
                    $"{signer.ToString()};" +
                    $"{txId.ToHex()};" +
                    $"{txNonce}");

            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.Flush();
                throw;
            }
        }

        private void StoreUpdatedAddressReferences<T>(
            Transaction<T> tx,
            StreamWriter updatedAddressRefBulkFile)
            where T : IAction, new()
        {
            try
            {
                foreach (Libplanet.Address address in tx.UpdatedAddresses)
                {
                    updatedAddressRefBulkFile.WriteLine(
                        $"{address.ToString()};" +
                        $"{tx.Id.ToString()};" +
                        $"{tx.Nonce}");
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.Flush();
                throw;
            }
        }

        public void WriteHeader(string dbName, StreamWriter bulkFile)
        {
            try
            {
                if (dbName == "block")
                {
                    bulkFile.WriteLine(
                        "index;" +
                        "hash;" +
                        "pre_evaluation_hash;" +
                        "state_root_hash;" +
                        "difficulty;" +
                        "total_difficulty;" +
                        "nonce;" +
                        "miner;" +
                        "previous_hash;" +
                        "timestamp;" +
                        "tx_hash;" +
                        "protocol_version");
                }
                else if (dbName == "transaction")
                {
                    bulkFile.WriteLine(
                        "tx_id;" +
                        "nonce;" +
                        "signer;" +
                        "signature;" +
                        "timestamp;" +
                        "public_key;" +
                        "genesis_hash;" +
                        "bytes_length");
                }
                else if (dbName == "tx_references")
                {
                    bulkFile.WriteLine(
                        "tx_id;" +
                        "block_hash;" +
                        "tx_nonce");
                }
                else if (dbName == "signer_references")
                {
                    bulkFile.WriteLine(
                        "signer;" +
                        "tx_id;" +
                        "tx_nonce");
                }
                else
                {
                    bulkFile.WriteLine(
                        "updated_address;" +
                        "tx_id;" +
                        "tx_nonce");
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error: {0}", e.Message);
                Console.Error.Flush();
                throw;
            }
        }

        private class DummyAction : IAction
        {
            public IValue PlainValue { get; private set; }
            public void LoadPlainValue(IValue plainValue) { PlainValue = plainValue; }
            public IAccountStateDelta Execute(IActionContext context) => context.PreviousStates;
            public void Render(IActionContext context, IAccountStateDelta nextStates) { }
            public void RenderError(IActionContext context, Exception exception) { }
            public void Unrender(IActionContext context, IAccountStateDelta nextStates) { }
            public void UnrenderError(IActionContext context, Exception exception) { }
        }
    }
}
