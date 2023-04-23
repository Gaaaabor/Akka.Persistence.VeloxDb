using Akka.Persistence.VeloxDb.Db;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    [DbAPI(Name = "Akka.Persistence.VeloxDb.Db.SnapshotStoreItemApi")]
    public interface ISnapshotStoreItemApi
    {
        [DbAPIOperation]
        string CreateSnapshotItem(SnapshotStoreItemDto snapshotItemDto);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        string GetLatestSnapshot(string persistenceId, long minSequenceNr, long maxSequenceNr, long fromTimestamp, long toTimestamp);

        [DbAPIOperation]
        void DeleteMessagesTo(string persistenceId, long sequenceNr);

        [DbAPIOperation]
        void DeleteMessagesTo(string persistenceId, long minSequenceNr, long maxSequenceNr);        
    }
}
