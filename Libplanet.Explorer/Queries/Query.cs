using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using GraphQL.Types;
using Libplanet.Action;
using Libplanet.Blockchain;
using Libplanet.Blocks;
using Libplanet.Explorer.GraphTypes;
using Libplanet.Explorer.Interfaces;
using Libplanet.Explorer.Store;
using Libplanet.Store;
using Libplanet.Tx;

namespace Libplanet.Explorer.Queries
{
    public class Query<T> : ObjectGraphType
        where T : IAction, new()
    {
        private static BlockChain<T> _chain;
        private static IStore _store;

        public Query(IBlockChainContext<T> chainContext)
        {
            Field<BlockQuery<T>>("blockQuery", resolve: context => new { });
            Field<TransactionQuery<T>>("transactionQuery", resolve: context => new { });
            Field<NonNullGraphType<NodeStateType<T>>>(
                "nodeState",
                resolve: context => chainContext
            );

            _chain = chainContext.BlockChain;
            _store = chainContext.Store;
            Name = "Query";
        }

        internal static IEnumerable<Block<T>> ListBlocks(
            bool desc,
            long offset,
            int? limit,
            bool excludeEmptyTxs,
            Address? miner)
        {
            Block<T> tip = _chain.Tip;
            long tipIndex = tip.Index;

            if (offset < 0)
            {
                offset = tipIndex + offset + 1;
            }

            if (tipIndex < offset || offset < 0)
            {
                yield break;
            }

            if (_store is MySQLRichStore mysqlStore)
            {
                IEnumerable<HashDigest<SHA256>> blockHashes;
                if (!excludeEmptyTxs)
                {
                    blockHashes = mysqlStore.GetBlockHashes(
                            desc, (int)offset, limit, miner
                        );
                }
                else
                {
                    blockHashes = mysqlStore.GetBlockHashesWithTx(
                            desc, (int)offset, limit, miner
                        );
                }

                IEnumerable<Block<T>> blocks = blockHashes
                    .Select(blockHash => _store.GetBlock<T>(blockHash));
                foreach (Block<T> blk in blocks)
                {
                    yield return blk;
                }

                yield break;
            }

            Block<T> block = desc ? _chain[tipIndex] : _chain[0];

            while (limit is null || limit > 0)
            {
                bool isMinerValid = miner is null || miner == block.Miner;
                bool isTxValid = !excludeEmptyTxs || block.Transactions.Any();

                if (isMinerValid && isTxValid)
                {
                    if (offset > 0)
                    {
                        offset--;
                    }
                    else
                    {
                        limit--;
                        yield return block;
                    }
                }

                block = GetNextBlock(block, desc);

                if (block is null)
                {
                    break;
                }
            }
        }

        internal static IEnumerable<Transaction<T>> ListTransactions(
            Address? signer, Address? involved, bool desc, long offset, int? limit)
        {
            Block<T> tip = _chain.Tip;
            long tipIndex = tip?.Index ?? -1;

            if (offset < 0)
            {
                offset = tipIndex + offset + 1;
            }

            if (tipIndex < offset || offset < 0)
            {
                yield break;
            }

            if (_store is IRichStore richStore)
            {
                IEnumerable<TxId> txIds;
                if (!(signer is null))
                {
                    txIds = richStore
                        .IterateSignerReferences(
                            (Address)signer, desc, (int)offset, limit ?? int.MaxValue);
                }
                else if (!(involved is null))
                {
                    txIds = richStore
                        .IterateUpdatedAddressReferences(
                            (Address)involved, desc, (int)offset, limit ?? int.MaxValue);
                }
                else
                {
                    txIds = richStore
                        .IterateTxReferences(null, desc, (int)offset, limit ?? int.MaxValue)
                        .Select(r => r.Item1);
                }

                var txs = txIds.Select(txId => _chain.GetTransaction(txId));
                foreach (var tx in txs)
                {
                    yield return tx;
                }

                yield break;
            }

            Block<T> block = _chain[desc ? tipIndex - offset : offset];

            while (!(block is null) && (limit is null || limit > 0))
            {
                foreach (var tx in desc ? block.Transactions.Reverse() : block.Transactions)
                {
                    if (IsValidTransaction(tx, signer, involved))
                    {
                        yield return tx;
                        limit--;
                        if (limit <= 0)
                        {
                            break;
                        }
                    }
                }

                block = GetNextBlock(block, desc);
            }
        }

        internal static IEnumerable<Transaction<T>> ListStagedTransactions(
            Address? signer, Address? involved, bool desc, int offset, int? limit)
        {
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(offset),
                    $"{nameof(ListStagedTransactions)} doesn't support negative offset.");
            }

            var stagedTxs = _chain.StagePolicy.Iterate(_chain)
                .Where(tx => IsValidTransaction(tx, signer, involved))
                .Skip(offset);

            stagedTxs = desc ? stagedTxs.OrderByDescending(tx => tx.Timestamp)
                : stagedTxs.OrderBy(tx => tx.Timestamp);

            stagedTxs = stagedTxs.TakeWhile((tx, index) => limit is null || index < limit);

            return stagedTxs;
        }

        internal static Block<T> GetBlockByHash(HashDigest<SHA256> hash)
        {
            return _store.GetBlock<T>(hash);
        }

        internal static Block<T> GetBlockByIndex(long index)
        {
            return _chain[index];
        }

        internal static Transaction<T> GetTransaction(TxId id)
        {
            return _chain.GetTransaction(id);
        }

        private static Block<T> GetNextBlock(Block<T> block, bool desc)
        {
            if (desc && block.PreviousHash is HashDigest<SHA256> prev)
            {
                return _chain[prev];
            }
            else if (!desc && block != _chain.Tip)
            {
                return _chain[block.Index + 1];
            }

            return null;
        }

        private static bool IsValidTransaction(
            Transaction<T> tx,
            Address? signer,
            Address? involved)
        {
            if (involved is null && signer is null)
            {
                return true;
            }
            else if (!(signer is null))
            {
                return tx.Signer.Equals(signer.Value);
            }
            else
            {
                return tx.UpdatedAddresses.Contains(involved.Value);
            }
        }
    }
}
