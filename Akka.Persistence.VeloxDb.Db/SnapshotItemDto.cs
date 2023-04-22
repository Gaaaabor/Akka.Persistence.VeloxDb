namespace Akka.Persistence.VeloxDb.Db
{
    public class SnapshotItemDto
    {
        public long Id { get; set; }
        public string PersistenceId { get; set; }
        public long SequenceNumber { get; set; }
        public long Timestamp { get; set; }
        public string Type { get; set; }
        public string Payload { get; set; }
    }
}
