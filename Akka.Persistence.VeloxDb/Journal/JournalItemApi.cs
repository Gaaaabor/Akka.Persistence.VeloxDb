using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Journal
{
    [DbAPI]
    public class JournalApi
    {
        [DbAPIOperation]
        public long CreateJournalItem(
            ObjectModel objectModel,
            long ordering,
            string persistenceId,
            long sequenceNumber,
            long highestSequenceNumber,
            long timestamp,
            string manifest,
            SerializationType serializationType,
            string payload,
            string payloadType,
            string tags,
            string writerGuid)
        {
            var journalItem = objectModel.CreateObject<JournalItem>();
            journalItem.Ordering = ordering;
            journalItem.PersistenceId = persistenceId;
            journalItem.SequenceNumber = sequenceNumber;
            journalItem.HighestSequenceNumber = highestSequenceNumber;
            journalItem.Timestamp = timestamp;
            journalItem.Manifest = manifest;
            journalItem.SerializationType = serializationType;
            journalItem.Payload = payload;
            journalItem.PayloadType = payloadType;            
            journalItem.Tags = tags;
            journalItem.WriterGuid = writerGuid;

            return journalItem.Id;
        }
    }

    [DbAPI(Name = "Akka.Persistence.VeloxDb.Journal.JournalApi")]
    public interface IJournalApi
    {
        [DbAPIOperation]
        long CreateJournalItem(
            long ordering,
            string persistenceId,
            long sequenceNumber,
            long highestSequenceNumber,
            long timestamp,
            string manifest,
            SerializationType serializationType,
            string payload,
            string payloadType,
            string tags,
            string writerGuid);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        Task<long> GetHighestSequenceNumberAsync(string persistenceId, long fromSequenceNr);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        Task<List<JournalItem>> GetMessagesRangeAsync(string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize);
    }
}
