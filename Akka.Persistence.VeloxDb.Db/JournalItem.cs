using Akka.Persistence.VeloxDb.Db;
using VeloxDB.ObjectInterface;

namespace Akka.Persistence.VeloxDb.Journal
{
    [DatabaseClass]
    public abstract partial class JournalItem : DatabaseObject
    {
        [DatabaseProperty]
        public abstract long Ordering { get; set; }

        [DatabaseProperty]
        public abstract string PersistenceId { get; set; }

        [DatabaseProperty]
        public abstract long SequenceNumber { get; set; }
        
        [DatabaseProperty]
        public abstract long HighestSequenceNumber { get; set; }        

        [DatabaseProperty]
        public abstract long Timestamp { get; set; }

        [DatabaseProperty]
        public abstract string Manifest { get; set; }

        [DatabaseProperty]
        public abstract SerializationType SerializationType { get; set; }

        [DatabaseProperty]
        public abstract string Payload { get; set; }

        [DatabaseProperty]
        public abstract string PayloadType { get; set; }

        [DatabaseProperty]
        public abstract string Tags { get; set; }

        [DatabaseProperty]
        public abstract string WriterGuid { get; set; }

        [DatabaseProperty]
        public abstract string GroupKey { get; set; }

        [DatabaseProperty]
        public abstract string Type { get; set; }

        [DatabaseProperty]
        public abstract string DocumentType { get; set; }

        [DatabaseProperty]
        public abstract string Tag { get; set; }
    }
}
