using VeloxDB.ObjectInterface;

namespace Akka.Persistence.VeloxDb.Journal
{
    [DatabaseClass]
    public abstract partial class JournalItem : DatabaseObject
    {
        [DatabaseProperty]
        public abstract string GroupKey { get; set; }

        [DatabaseProperty]
        public abstract string PersistenceId { get; set; }

        [DatabaseProperty]
        public abstract long SequenceNumber { get; set; }

        [DatabaseProperty]
        public abstract string DocumentType { get; set; }

        [DatabaseProperty]
        public abstract string Manifest { get; set; }

        [DatabaseProperty]
        public abstract string WriterGuid { get; set; }

        [DatabaseProperty]
        public abstract long Timestamp { get; set; }

        [DatabaseProperty]
        public abstract string Type { get; set; }        

        [DatabaseProperty]
        public abstract DatabaseArray<byte> Payload { get; set; }

        [DatabaseProperty]
        public abstract string Tag { get; set; }

        [DatabaseProperty]
        public abstract long HighestSequenceNumber { get; set; }

        [DatabaseProperty]
        public abstract bool IsSoftDeleted { get; set; }
    }
}
