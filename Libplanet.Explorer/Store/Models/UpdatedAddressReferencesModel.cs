namespace MySqlStore.Models
{
    public class UpdatedAddressReferencesModel
    {
        public byte[] TxId { get; set; }

        public long TxNonce { get; set; }

        public byte[] UpdatedAddress { get; set; }
    }
}
