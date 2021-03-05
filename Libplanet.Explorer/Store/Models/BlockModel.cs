namespace MySqlStore.Models
{
    public class BlockModel
    {
        public int Index { get; set; }

        public byte[] Hash { get; set; }

        public byte[] PreEvaluationHash { get; set; }

        public byte[] StateRootHash { get; set; }

        public long Difficulty { get; set; }

        public long TotalDifficulty { get; set; }

        public byte[] Nonce { get; set; }

        public byte[] Miner { get; set; }

        public byte[] PreviousHash { get; set; }

        public string Timestamp { get; set; }

        public byte[] TxHash { get; set; }

        public int ByteLength { get; set; }
    }
}
