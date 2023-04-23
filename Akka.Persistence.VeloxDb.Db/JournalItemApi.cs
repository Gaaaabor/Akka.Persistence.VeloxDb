using Akka.Persistence.VeloxDb.Journal;
using System.Text;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI]
    public class JournalItemApi
    {
        [DbAPIOperation]
        public string CreateJournalItem(ObjectModel objectModel, JournalItemDto journalItemDto)
        {
            var journalItem = objectModel.CreateObject<JournalItem>();

            journalItem.GroupKey = journalItemDto.GroupKey;
            journalItem.PersistenceId = journalItemDto.PersistenceId;
            journalItem.SequenceNumber = journalItemDto.SequenceNumber;
            journalItem.DocumentType = journalItemDto.DocumentType;
            journalItem.Manifest = journalItemDto.Manifest;
            journalItem.WriterGuid = journalItemDto.WriterGuid;
            journalItem.Timestamp = journalItemDto.Timestamp;
            journalItem.Type = journalItemDto.Type;
            journalItem.Payload = Serialize(journalItemDto.Payload);
            journalItem.Tag = journalItemDto.Tag;
            journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;

            return journalItem.PersistenceId;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public long GetHighestSequenceNumber(ObjectModel objectModel, string persistenceId, long fromSequenceNr)
        {
            var items = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr);

            if (items is null || !items.Any())
            {
                return 0;
            }

            var highestSequenceNumber = items.Max(x => x.SequenceNumber);

            return highestSequenceNumber;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public List<JournalItemDto> GetJournalItems(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize)
        {
            var journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr && x.SequenceNumber <= toSequenceNr)
                .Take(pageSize)
                .Select(x => new JournalItemDto
                {
                    GroupKey = x.GroupKey,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    DocumentType = x.DocumentType,
                    Manifest = x.Manifest,
                    WriterGuid = x.WriterGuid,
                    Timestamp = x.Timestamp,
                    Type = x.Type,
                    Payload = Deserialize(x.Payload),
                    Tag = x.Tag,
                    HighestSequenceNumber = x.HighestSequenceNumber
                })
                .ToList();

            return journalItems ?? new List<JournalItemDto>();
        }

        [DbAPIOperation]
        public void DeleteJournalItemsTo(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            var journalItems = objectModel
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

            journalItem.GroupKey = journalItemDto.GroupKey;
            journalItem.PersistenceId = journalItemDto.PersistenceId;
            journalItem.SequenceNumber = journalItemDto.SequenceNumber;
            journalItem.DocumentType = journalItemDto.DocumentType;
            journalItem.Manifest = journalItemDto.Manifest;
            journalItem.WriterGuid = journalItemDto.WriterGuid;
            journalItem.Timestamp = journalItemDto.Timestamp;
            journalItem.Type = journalItemDto.Type;
            journalItem.Payload = Serialize(journalItemDto.Payload);
            journalItem.Tag = journalItemDto.Tag;
            journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;

            objectModel.ApplyChanges();
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public List<JournalItemDto> GetTaggedJournalItems(ObjectModel objectModel, string tag, long fromOffset, long toOffset, long max)
        {
            var take = max > int.MaxValue ? int.MaxValue : int.Parse(max.ToString());

            var journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.Tag == tag && fromOffset <= x.Timestamp && x.Timestamp <= toOffset)
                .Take(take)
                .Select(x => new JournalItemDto
                {
                    GroupKey = x.GroupKey,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    DocumentType = x.DocumentType,
                    Manifest = x.Manifest,
                    WriterGuid = x.WriterGuid,
                    Timestamp = x.Timestamp,
                    Type = x.Type,
                    Payload = Deserialize(x.Payload),
                    Tag = x.Tag,
                    HighestSequenceNumber = x.HighestSequenceNumber
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

        private static byte[] Deserialize(string payload)
        {
            if (payload is null)
            {
                return null;
            }

            return Encoding.UTF8.GetBytes(payload);
        }

        private static string Serialize(byte[] payload)
        {
            if (payload is null)
            {
                return null;
            }

            return Encoding.UTF8.GetString(payload);
        }
    }
}