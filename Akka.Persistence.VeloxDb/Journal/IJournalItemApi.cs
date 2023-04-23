using Akka.Persistence.VeloxDb.Db;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Journal
{
    [DbAPI(Name = "Akka.Persistence.VeloxDb.Db.JournalItemApi")]
    public interface IJournalItemApi
    {
        [DbAPIOperation]
        string CreateJournalItem(JournalItemDto journalItemDto);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        long GetHighestSequenceNumber(string persistenceId, long fromSequenceNr);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        List<JournalItemDto> GetJournalItems(string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize);

        [DbAPIOperation]
        void DeleteJournalItemsTo(string persistenceId, long fromSequenceNr, long toSequenceNr);

        [DbAPIOperation]
        void UpdateJournalItem(long id, JournalItemDto journalItemDto);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        List<JournalItemDto> GetTaggedJournalItems(string tag, long fromOffset, long toOffset, long max);

        [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
        List<string> GetPersistenceIds();
    }
}
