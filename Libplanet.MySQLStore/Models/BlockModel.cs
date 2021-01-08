namespace MySqlStore.Models
{
    public class BlockModel
    {
        public int Index { get; set; }

        public string Hash { get; set; }

        public string Pre_evaluation_hash { get; set; }

        public string State_root_hash { get; set; }

        public long Difficulty { get; set; }

        public long Total_difficulty { get; set; }

        public string Nonce { get; set; }

        public string Miner { get; set; }

        public string Previous_hash { get; set; }

        public string Timestamp { get; set; }

        public string Tx_hash { get; set; }

        public string Byte_length { get; set; }

        public byte[] Key { get; set; }

        public byte[] Value { get; set; }
    }
}
