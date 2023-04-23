using VeloxDB.ObjectInterface;

namespace Akka.Persistence.VeloxDb.Db
{
    [DatabaseClass]
    public abstract partial class SnapshotStoreItem : DatabaseObject
    {
        [DatabaseProperty]
        public abstract string PersistenceId { get; set; }

        [DatabaseProperty]
        public abstract long SequenceNumber { get; set; }

        [DatabaseProperty]
        public abstract long Timestamp { get; set; }

        [DatabaseProperty]
        public abstract string Type { get; set; }

        [DatabaseProperty]
        public abstract string Payload { get; set; }
    }
}
