namespace Akka.Persistence.VeloxDb.Db
{
    public class SnapshotStoreItemDto
    {
        public long Id { get; set; }
        public string PersistenceId { get; set; }
        public long SequenceNumber { get; set; }
        public long Timestamp { get; set; }
        public string Type { get; set; }
        public byte[] Payload { get; set; }
    }
}
