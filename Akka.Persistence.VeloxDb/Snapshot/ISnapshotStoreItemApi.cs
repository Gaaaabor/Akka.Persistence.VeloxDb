using Akka.Persistence.VeloxDb.Db;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    [DbAPI(Name = "Akka.Persistence.VeloxDb.Db.SnapshotStoreItemApi")]
    public interface ISnapshotStoreItemApi
    {
        [DbAPIOperation]
        void CreateSnapshotItem(SnapshotStoreItemDto snapshotStoreItemDto);

        [DbAPIOperation]
        SnapshotStoreItemDto GetLatestSnapshotItemRange(string persistenceId, long fromSequenceNumber, long toSequenceNumber, long fromTimestamp, long toTimestamp);

        [DbAPIOperation]
        void DeleteSnapshotItem(string persistenceId, long sequenceNumber, long timestamp);

        [DbAPIOperation]
        void DeleteSnapshotItemsRange(string persistenceId, long fromSequenceNumber, long toSequenceNumber, long fromTimestamp, long toTimestamp);

        [DbAPIOperation]
        void Flush();
    }
}
