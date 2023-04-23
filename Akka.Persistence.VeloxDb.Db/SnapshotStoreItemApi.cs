using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI(Name = "SnapshotStoreItemApi")]
    public class SnapshotStoreItemApi
    {
        [DbAPIOperation]
        public string CreateSnapshotItem(ObjectModel objectModel, SnapshotStoreItemDto snapshotStoreItemDto)
        {
            var snapshotStoreItem = objectModel.CreateObject<SnapshotStoreItem>();

            snapshotStoreItem.PersistenceId = snapshotStoreItemDto.PersistenceId;
            snapshotStoreItem.SequenceNumber = snapshotStoreItemDto.SequenceNumber;
            snapshotStoreItem.Timestamp = snapshotStoreItemDto.Timestamp;
            snapshotStoreItem.Payload = snapshotStoreItemDto.Payload;
            snapshotStoreItem.Type = snapshotStoreItemDto.Type;

            return snapshotStoreItem.PersistenceId;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public SnapshotStoreItemDto GetLatestSnapshot(ObjectModel objectModel, string persistenceId, long minSequenceNr, long maxSequenceNr, long fromTimestamp, long toTimestamp)
        {
            var snapshotStoreItem = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .FirstOrDefault(x =>
                    x.PersistenceId == persistenceId &&
                    minSequenceNr <= x.SequenceNumber &&
                    x.SequenceNumber <= maxSequenceNr &&
                    fromTimestamp <= x.Timestamp &&
                    x.Timestamp <= toTimestamp);

            if (snapshotStoreItem is null)
            {
                return null;
            }

            return new SnapshotStoreItemDto
            {
                Id = snapshotStoreItem.Id,
                Payload = snapshotStoreItem.Payload,
                PersistenceId = snapshotStoreItem.PersistenceId,
                SequenceNumber = snapshotStoreItem.SequenceNumber,
                Timestamp = snapshotStoreItem.Timestamp,
                Type = snapshotStoreItem.Type
            };
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long toSequenceNr)
        {
            DeleteMessagesTo(objectModel, persistenceId, 0, toSequenceNr);
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            var snapshotStoreItems = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .Where(x => x.PersistenceId == persistenceId && fromSequenceNr <= x.SequenceNumber && x.SequenceNumber <= toSequenceNr);

            foreach (var snapshotStoreItem in snapshotStoreItems)
            {
                snapshotStoreItem.Delete();
            }

            objectModel.ApplyChanges();
        }
    }
}