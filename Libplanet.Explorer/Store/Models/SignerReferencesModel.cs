namespace MySqlStore.Models
{
    public class SignerReferencesModel
    {
        public byte[] TxId { get; set; }

        public long TxNonce { get; set; }

        public byte[] Signer { get; set; }
    }
}
