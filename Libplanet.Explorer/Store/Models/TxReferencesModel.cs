namespace MySqlStore.Models
{
    public class TxReferencesModel
    {
        public byte[] TxId { get; set; }

        public long TxNonce { get; set; }

        public byte[] BlockHash { get; set; }
    }
}
