using Akka.Persistence.VeloxDb.Db;
using System.Collections.Immutable;
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
        string GetMessagesRange(string persistenceId, long fromSequenceNr, long toSequenceNr, int pageSize);

        [DbAPIOperation]
        void DeleteMessagesTo(string persistenceId, long toSequenceNr);
        
        //[DbAPIOperation]
        //void WriteMessages(IEnumerable<AtomicWrite> messages);
    }
}
