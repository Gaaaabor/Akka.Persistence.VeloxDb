using Akka.Persistence.VeloxDb.Journal;
using System.Text.Json;
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

            return journalItem.PersistenceId;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public long GetHighestSequenceNumber(ObjectModel objectModel, string persistenceId, long fromSequenceNr)
        {
            IEnumerable<JournalItem> journalItems = objectModel.GetAllObjects<JournalItem>();

            var highestSequenceNumber = journalItems.Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr).Max(x => x.SequenceNumber);
            return highestSequenceNumber;
        }

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        public string GetMessagesRange(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize)
        {
            IEnumerable<JournalItem> journalItems = objectModel.GetAllObjects<JournalItem>();

            var items = journalItems
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber >= fromSequenceNr && x.SequenceNumber <= toSequenceNr)
                .Take(pageSize)
                .ToList();

            return JsonSerializer.Serialize(items);
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
    }
}