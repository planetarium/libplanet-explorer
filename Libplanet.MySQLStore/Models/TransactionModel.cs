namespace MySQLStore.Models
{
    public class TransactionModel
    {
        public string Id { get; set; }

        public string Nonce { get; set; }

        public string Signer { get; set; }

        public string Signature { get; set; }

        public string Timestamp { get; set; }

        public string Public_key { get; set; }

        public string Genesis_hash { get; set; }

        public string Bytes_length { get; set; }

        public byte[] Key { get; set; }

        public byte[] Value { get; set; }
    }
}
