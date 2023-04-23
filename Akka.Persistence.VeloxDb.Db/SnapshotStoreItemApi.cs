using System.Text.Json;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI]
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
        public string GetLatestSnapshot(ObjectModel objectModel, string persistenceId, long minSequenceNr, long maxSequenceNr, long fromTimestamp, long toTimestamp)
        {
            IEnumerable<SnapshotStoreItem> snapshotStoreItems = objectModel.GetAllObjects<SnapshotStoreItem>();

            var result = snapshotStoreItems.FirstOrDefault(x =>
                x.PersistenceId == persistenceId &&
                minSequenceNr <= x.SequenceNumber &&
                x.SequenceNumber <= maxSequenceNr &&
                fromTimestamp <= x.Timestamp &&
                x.Timestamp <= toTimestamp);

            if (result is null)
            {
                return null;
            }

            return JsonSerializer.Serialize(new SnapshotStoreItemDto
            {
                Id = result.Id,
                Payload = result.Payload,
                PersistenceId = result.PersistenceId,
                SequenceNumber = result.SequenceNumber,
                Timestamp = result.Timestamp,
                Type = result.Type
            });
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long toSequenceNr)
        {
            IEnumerable<SnapshotStoreItem> snapshotStoreItems = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber <= toSequenceNr);

            foreach (var snapshotItem in snapshotStoreItems)
            {
                snapshotItem.Delete();
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            IEnumerable<SnapshotStoreItem> snapshotStoreItems = objectModel
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