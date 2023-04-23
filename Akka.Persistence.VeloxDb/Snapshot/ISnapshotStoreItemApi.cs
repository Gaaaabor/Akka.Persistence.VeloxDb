using Akka.Persistence.VeloxDb.Db;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    [DbAPI(Name = "Akka.Persistence.VeloxDb.Db.SnapshotStoreItemApi")]
    public interface ISnapshotStoreItemApi
    {
        [DbAPIOperation]
        string CreateSnapshotItem(SnapshotStoreItemDto snapshotStoreItemDto);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        string GetLatestSnapshot(string persistenceId, long minSequenceNr, long maxSequenceNr, long fromTimestamp, long toTimestamp);

        [DbAPIOperation]
        void DeleteMessagesTo(string persistenceId, long toSequenceNr);

        [DbAPIOperation]
        void DeleteMessagesTo(string persistenceId, long fromSequenceNr, long toSequenceNr);        
    }
}
