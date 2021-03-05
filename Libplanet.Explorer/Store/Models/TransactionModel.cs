namespace MySqlStore.Models
{
    public class TransactionModel
    {
        public byte[] TxId { get; set; }

        public long Nonce { get; set; }

        public byte[] Signer { get; set; }

        public byte[] Signature { get; set; }

        public string Timestamp { get; set; }

        public string PublicKey { get; set; }

        public byte[] GenesisHash { get; set; }

        public int BytesLength { get; set; }
    }
}
