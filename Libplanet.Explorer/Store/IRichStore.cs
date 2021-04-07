using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Libplanet.Action;
using Libplanet.Store;
using Libplanet.Tx;

namespace Libplanet.Explorer.Store
{
    public interface IRichStore : IStore
    {
        void StoreTxReferences(TxId txId, HashDigest<SHA256> blockHash, long blockIndex);

        IEnumerable<ValueTuple<TxId, HashDigest<SHA256>>> IterateTxReferences(
            TxId? txId = null,
            bool desc = false,
            int offset = 0,
            int limit = int.MaxValue);

        void StoreSignerReferences(TxId txId, long txNonce, Address signer);

        IEnumerable<TxId> IterateSignerReferences(
            Address signer,
            bool desc,
            int offset = 0,
            int limit = int.MaxValue);

        void StoreUpdatedAddressReferences<T>(Transaction<T> tx)
            where T : IAction, new();

        IEnumerable<TxId> IterateUpdatedAddressReferences(
            Address updatedAddress,
            bool desc,
            int offset = 0,
            int limit = int.MaxValue);
    }
}
