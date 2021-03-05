namespace MySqlStore.Models
{
    public class TransactionModel
    {
        public string TxId { get; set; }

        public string Nonce { get; set; }

        public string Signer { get; set; }

        public string Signature { get; set; }

        public string Timestamp { get; set; }

        public string PublicKey { get; set; }

        public string GenesisHash { get; set; }

        public string BytesLength { get; set; }

        public byte[] Key { get; set; }

        public byte[] Value { get; set; }
    }
}
