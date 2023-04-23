using System.Text.Json;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI]
    public class SnapshotStoreItemApi
    {
        [DbAPIOperation]
        public string CreateSnapshotItem(ObjectModel objectModel, SnapshotStoreItemDto snapshotItemDto)
        {
            var snapshotStoreItem = objectModel.CreateObject<SnapshotStoreItem>();

            snapshotStoreItem.PersistenceId = snapshotItemDto.PersistenceId;
            snapshotStoreItem.SequenceNumber = snapshotItemDto.SequenceNumber;
            snapshotStoreItem.Timestamp = snapshotItemDto.Timestamp;
            snapshotStoreItem.Payload = snapshotItemDto.Payload;
            snapshotStoreItem.Type = snapshotItemDto.Type;

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

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public string GetMessagesRange(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize)
        {
            IEnumerable<SnapshotStoreItem> snapshotStoreItems = objectModel.GetAllObjects<SnapshotStoreItem>();

            var items = snapshotStoreItems
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr && x.SequenceNumber <= toSequenceNr)
                .Take(pageSize)
                .Select(x => new SnapshotStoreItemDto
                {
                    Id = x.Id,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    Timestamp = x.Timestamp,
                    Payload = x.Payload,
                    Type = x.Type
                })
                .ToList();

            //return items;
            return JsonSerializer.Serialize(items);
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

        [DbAPIOperation]
        public void UpdateJournalItem(ObjectModel objectModel, long id, JournalItemDto journalItemDto)
        {
            var snapshotStoreItem = objectModel.GetObject<SnapshotStoreItem>(id);

            if (snapshotStoreItem is null)
            {
                return;
            }
            
            snapshotStoreItem.PersistenceId = journalItemDto.PersistenceId;
            snapshotStoreItem.SequenceNumber = journalItemDto.SequenceNumber;            
            snapshotStoreItem.Timestamp = journalItemDto.Timestamp;            
            snapshotStoreItem.Payload = journalItemDto.Payload;
            snapshotStoreItem.Type = journalItemDto.Type;

            objectModel.ApplyChanges();
        }
    }
}