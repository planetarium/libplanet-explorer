namespace MySqlStore.Models
{
    public class TransactionModel
    {
        public string TxId { get; set; }

        public long Nonce { get; set; }

        public string Signer { get; set; }

        public string Signature { get; set; }

        public string Timestamp { get; set; }

        public string PublicKey { get; set; }

        public string GenesisHash { get; set; }

        public int BytesLength { get; set; }
    }
}
