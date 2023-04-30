using Akka.Persistence.VeloxDb.Db;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Journal
{
    [DbAPI(Name = "Akka.Persistence.VeloxDb.Db.JournalItemApi")]
    public interface IJournalItemApi
    {
        [DbAPIOperation]
        void CreateJournalItem(JournalItemDto journalItemDto);

        [DbAPIOperation]
        void CreateJournalItemBatch(List<JournalItemDto> journalItemDtos);

        [DbAPIOperation]
        long GetHighestSequenceNumber(string persistenceId, long fromSequenceNr);

        [DbAPIOperation]
        List<JournalItemDto> GetJournalItemsRange(string persistenceId, long fromSequenceNr, long toSequenceNr, string groupKey);

        [DbAPIOperation]
        void DeleteJournalItemsTo(string persistenceId, long toSequenceNr);

        [DbAPIOperation]
        void UpdateJournalItem(string persistenceId, JournalItemDto journalItemDto);

        [DbAPIOperation]
        List<JournalItemDto> GetTaggedJournalItems(string tag, long fromOffset, long toOffset);

        [DbAPIOperation]
        List<string> GetPersistenceIds();

        [DbAPIOperation]
        void Flush();
    }
}
