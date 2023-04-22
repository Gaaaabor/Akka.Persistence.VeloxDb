namespace Akka.Persistence.VeloxDb.Db
{
    public class JournalItemDto
    {
        public long Id { get; set; }
        public long Ordering { get; set; }
        public string PersistenceId { get; set; }
        public long SequenceNumber { get; set; }
        public long HighestSequenceNumber { get; set; }
        public long Timestamp { get; set; }
        public string Manifest { get; set; }
        public SerializationType SerializationType { get; set; }
        public string Payload { get; set; }
        public string PayloadType { get; set; }
        public string Tags { get; set; }
        public string WriterGuid { get; set; }
        public string GroupKey { get; set; }
        public string Type { get; set; }
        public string DocumentType { get; set; }
        public string Tag { get; set; }
    }
}
