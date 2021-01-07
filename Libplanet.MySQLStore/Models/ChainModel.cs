namespace MySQLStore.Models
{
    public class ChainModel
    {
        public byte[] Key { get; set; }

        public byte[] Value { get; set; }

        public string Cf { get; set; }

        public byte[] Prefix { get; set; }
    }
}
