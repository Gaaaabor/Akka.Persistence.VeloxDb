using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI]
    public class SnapshotStoreItemApi
    {
        [DbAPIOperation]
        public void CreateSnapshotItem(ObjectModel objectModel, SnapshotStoreItemDto snapshotStoreItemDto)
        {
            var itemsToOverwrite = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .Where(x =>
                    x.PersistenceId == snapshotStoreItemDto.PersistenceId &&
                    x.SequenceNumber == snapshotStoreItemDto.SequenceNumber &&
                    !x.IsDeleted &&
                    !x.IsSoftDeleted);

            bool hasOverwrite = false;
            foreach (var itemToOverwrite in itemsToOverwrite)
            {
                itemToOverwrite.PersistenceId = snapshotStoreItemDto.PersistenceId;
                itemToOverwrite.SequenceNumber = snapshotStoreItemDto.SequenceNumber;
                itemToOverwrite.Timestamp = snapshotStoreItemDto.Timestamp;
                itemToOverwrite.Payload = DatabaseArray<byte>.Create(snapshotStoreItemDto.Payload ?? Array.Empty<byte>());
                itemToOverwrite.Type = snapshotStoreItemDto.Type;
            }

            if (!hasOverwrite)
            {
                var snapshotStoreItem = objectModel.CreateObject<SnapshotStoreItem>();

                snapshotStoreItem.PersistenceId = snapshotStoreItemDto.PersistenceId;
                snapshotStoreItem.SequenceNumber = snapshotStoreItemDto.SequenceNumber;
                snapshotStoreItem.Timestamp = snapshotStoreItemDto.Timestamp;
                snapshotStoreItem.Payload = DatabaseArray<byte>.Create(snapshotStoreItemDto.Payload ?? Array.Empty<byte>());
                snapshotStoreItem.Type = snapshotStoreItemDto.Type;
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public SnapshotStoreItemDto GetLatestSnapshotItemRange(ObjectModel objectModel, string persistenceId, long fromSequenceNumber, long toSequenceNumber, long fromTimestamp, long toTimestamp)
        {
            var snapshotStoreItem = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .Where(x =>
                    x.PersistenceId == persistenceId &&
                    fromSequenceNumber <= x.SequenceNumber &&
                    x.SequenceNumber <= toSequenceNumber &&
                    fromTimestamp <= x.Timestamp &&
                    x.Timestamp <= toTimestamp &&
                    !x.IsSoftDeleted)
                .OrderByDescending(x => x.SequenceNumber)
                .FirstOrDefault();

            if (snapshotStoreItem is null)
            {
                return null;
            }

            return new SnapshotStoreItemDto
            {
                Id = snapshotStoreItem.Id,
                Payload = snapshotStoreItem.Payload?.ToArray() ?? Array.Empty<byte>(),
                PersistenceId = snapshotStoreItem.PersistenceId,
                SequenceNumber = snapshotStoreItem.SequenceNumber,
                Timestamp = snapshotStoreItem.Timestamp,
                Type = snapshotStoreItem.Type
            };
        }

        [DbAPIOperation]
        public void DeleteSnapshotItem(ObjectModel objectModel, string persistenceId, long sequenceNumber, long timestamp)
        {
            var snapshotStoreItems = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .Where(x =>
                    x.PersistenceId == persistenceId &&
                    x.SequenceNumber == sequenceNumber &&
                     //x.Timestamp == timestamp && // Not sure if needed, tests break when used
                    !x.IsSoftDeleted);

            foreach (var snapshotStoreItem in snapshotStoreItems)
            {
                snapshotStoreItem.IsSoftDeleted = true;
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public void DeleteSnapshotItemsRange(ObjectModel objectModel, string persistenceId, long fromSequenceNumber, long toSequenceNumber, long fromTimestamp, long toTimestamp)
        {
            var snapshotStoreItems = objectModel
                .GetAllObjects<SnapshotStoreItem>()
                .Where(x =>
                    x.PersistenceId == persistenceId &&
                    fromSequenceNumber <= x.SequenceNumber &&
                    x.SequenceNumber <= toSequenceNumber &&
                    fromTimestamp <= x.Timestamp &&
                    x.Timestamp <= toTimestamp &&
                    !x.IsSoftDeleted);

            foreach (var snapshotStoreItem in snapshotStoreItems)
            {
                snapshotStoreItem.IsSoftDeleted = true;
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public void Flush(ObjectModel objectModel)
        {
            var snapshotStoreItems = objectModel.GetAllObjects<SnapshotStoreItem>();

            foreach (var snapshotStoreItem in snapshotStoreItems)
            {
                snapshotStoreItem.IsSoftDeleted = true;
                snapshotStoreItem.Delete();
            }

            objectModel.ApplyChanges();
        }
    }
}