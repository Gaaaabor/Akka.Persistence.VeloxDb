using Akka.Persistence.VeloxDb.Journal;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI(Name = "JournalItemApi")]
    public class JournalItemApi
    {
        [DbAPIOperation]
        public string CreateJournalItem(ObjectModel objectModel, JournalItemDto journalItemDto)
        {
            var journalItem = objectModel.CreateObject<JournalItem>();

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

            return journalItem.PersistenceId;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public long GetHighestSequenceNumber(ObjectModel objectModel, string persistenceId, long fromSequenceNr)
        {
            var highestSequenceNumber = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr)
                .Max(x => x.SequenceNumber);

            return highestSequenceNumber;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public List<JournalItemDto> GetMessagesRange(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize)
        {
            var journalItems = objectModel
                .GetAllObjects<JournalItem>()
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

            return journalItems;
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long toSequenceNr)
        {
            IEnumerable<JournalItem> journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber <= toSequenceNr);

            foreach (var journalItem in journalItems)
            {
                journalItem.Delete();
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public void DeleteMessagesTo(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            IEnumerable<JournalItem> journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && fromSequenceNr <= x.SequenceNumber && x.SequenceNumber <= toSequenceNr);

            foreach (var journalItem in journalItems)
            {
                journalItem.Delete();
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
        public List<JournalItemDto> ReplayTaggedMessages(ObjectModel objectModel, string tag, long fromOffset, long toOffset, long max)
        {
            var take = max > int.MaxValue ? int.MaxValue : int.Parse(max.ToString());

            var journalItems = objectModel
                .GetAllObjects<JournalItem>()
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

            return journalItems;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public List<string> GetPersistenceIds(ObjectModel objectModel)
        {
            var highestSequenceNumberPropertyName = nameof(JournalItem.HighestSequenceNumber);

            var persistenceIds = objectModel.GetAllObjects<JournalItem>()
                .Where(x => x.DocumentType == highestSequenceNumberPropertyName)
                .Select(x => x.PersistenceId)
                .ToList();

            return persistenceIds;
        }
    }
}