using Akka.Persistence.VeloxDb.Journal;
using System.Text.Json;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI]
    public class SnapshotItemApi
    {
        [DbAPIOperation]
        public string CreateSnapshotItem(ObjectModel objectModel, SnapshotItemDto snapshotItemDto)
        {
            var snapshotItem = objectModel.CreateObject<SnapshotItem>();

            snapshotItem.PersistenceId = snapshotItemDto.PersistenceId;
            snapshotItem.SequenceNumber = snapshotItemDto.SequenceNumber;
            snapshotItem.Timestamp = snapshotItemDto.Timestamp;
            snapshotItem.Payload = snapshotItemDto.Payload;
            snapshotItem.Type = snapshotItemDto.Type;

            return snapshotItem.PersistenceId;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public string GetLatestSnapshot(ObjectModel objectModel, string persistenceId, long minSequenceNr, long maxSequenceNr, long fromTimestamp, long toTimestamp)
        {
            IEnumerable<SnapshotItem> snapshotItems = objectModel.GetAllObjects<SnapshotItem>();

            var result = snapshotItems.FirstOrDefault(x =>
                x.PersistenceId == persistenceId &&
                minSequenceNr <= x.SequenceNumber &&
                x.SequenceNumber <= maxSequenceNr &&
                fromTimestamp <= x.Timestamp &&
                x.Timestamp <= toTimestamp);

            if (result is null)
            {
                return null;
            }

            return JsonSerializer.Serialize(new SnapshotItemDto
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
            IEnumerable<JournalItem> journalItems = objectModel.GetAllObjects<JournalItem>();

            var items = journalItems
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr && x.SequenceNumber <= toSequenceNr)
                .Take(pageSize)
                .Select(x => new JournalItemDto
                {
                    Id = x.Id,
                    Ordering = x.Ordering,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    HighestSequenceNumber = x.HighestSequenceNumber,
                    Timestamp = x.Timestamp,
                    Manifest = x.Manifest,
                    SerializationType = x.SerializationType,
                    Payload = x.Payload,
                    PayloadType = x.PayloadType,
                    Tags = x.Tags,
                    WriterGuid = x.WriterGuid,
                    GroupKey = x.GroupKey,
                    Type = x.Type,
                    DocumentType = x.DocumentType,
                    Tag = x.Tag
                })
                .ToList();

            //return items;
            return JsonSerializer.Serialize(items);
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long toSequenceNr)
        {
            IEnumerable<SnapshotItem> snapshotItems = objectModel
                .GetAllObjects<SnapshotItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber <= toSequenceNr);

            foreach (var snapshotItem in snapshotItems)
            {
                snapshotItem.Delete();
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            IEnumerable<SnapshotItem> snapshotItems = objectModel
                .GetAllObjects<SnapshotItem>()
                .Where(x => x.PersistenceId == persistenceId && fromSequenceNr <= x.SequenceNumber && x.SequenceNumber <= toSequenceNr);

            foreach (var snapshotItem in snapshotItems)
            {
                snapshotItem.Delete();
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public void UpdateJournalItem(ObjectModel objectModel, long id, JournalItemDto journalItemDto)
        {
            var journalItem = objectModel.GetObject<JournalItem>(id);

            if (journalItem is null)
            {
                return;
            }

            journalItem.Ordering = journalItemDto.Ordering;
            journalItem.PersistenceId = journalItemDto.PersistenceId;
            journalItem.SequenceNumber = journalItemDto.SequenceNumber;
            journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;
            journalItem.Timestamp = journalItemDto.Timestamp;
            journalItem.Manifest = journalItemDto.Manifest;
            journalItem.SerializationType = journalItemDto.SerializationType;
            journalItem.Payload = journalItemDto.Payload;
            journalItem.PayloadType = journalItemDto.PayloadType;
            journalItem.Tags = journalItemDto.Tags;
            journalItem.WriterGuid = journalItemDto.WriterGuid;
            journalItem.GroupKey = journalItemDto.GroupKey;
            journalItem.Type = journalItemDto.Type;
            journalItem.DocumentType = journalItemDto.DocumentType;
            journalItem.Tag = journalItemDto.Tag;

            objectModel.ApplyChanges();
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public string ReplayTaggedMessages(ObjectModel objectModel, string tag, long fromOffset, long toOffset, long max)
        {
            IEnumerable<JournalItem> journalItems = objectModel.GetAllObjects<JournalItem>();

            var take = max > int.MaxValue ? int.MaxValue : int.Parse(max.ToString());

            var items = journalItems
                .Where(x => x.Tag == tag && fromOffset <= x.Timestamp && x.Timestamp <= toOffset)
                .Take(take)
                .Select(x => new JournalItemDto
                {
                    Id = x.Id,
                    Ordering = x.Ordering,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    HighestSequenceNumber = x.HighestSequenceNumber,
                    Timestamp = x.Timestamp,
                    Manifest = x.Manifest,
                    SerializationType = x.SerializationType,
                    Payload = x.Payload,
                    PayloadType = x.PayloadType,
                    Tags = x.Tags,
                    WriterGuid = x.WriterGuid,
                    GroupKey = x.GroupKey,
                    Type = x.Type,
                    DocumentType = x.DocumentType,
                    Tag = x.Tag
                })
                .ToList();

            return JsonSerializer.Serialize(items);
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public string GetPersistenceIds(ObjectModel objectModel)
        {
            IEnumerable<JournalItem> journalItems = objectModel.GetAllObjects<JournalItem>();

            var highestSequenceNumberPropertyName = nameof(JournalItem.HighestSequenceNumber);

            var items = journalItems
                .Where(x => x.DocumentType == highestSequenceNumberPropertyName)
                .Select(x => x.PersistenceId)
                .ToList();

            return JsonSerializer.Serialize(items);
        }
    }
}