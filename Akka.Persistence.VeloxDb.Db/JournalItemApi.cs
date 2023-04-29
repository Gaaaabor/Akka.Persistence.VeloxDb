using Akka.Persistence.VeloxDb.Journal;
using VeloxDB.Common;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;

namespace Akka.Persistence.VeloxDb.Db
{
    [DbAPI]
    public class JournalItemApi
    {
        [DbAPIOperation]
        public void CreateJournalItem(ObjectModel objectModel, JournalItemDto journalItemDto)
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
            journalItem.Payload = DatabaseArray<byte>.Create(journalItemDto.Payload ?? Array.Empty<byte>());
            journalItem.Tag = journalItemDto.Tag;
            journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;

            objectModel.ApplyChanges();

            APITrace.Warning("CreateJournalItem PersistenceId: {0}, GroupKey: {1}", journalItem.PersistenceId, journalItem.GroupKey);
        }

        [DbAPIOperation]
        public long GetHighestSequenceNumber(ObjectModel objectModel, string persistenceId, long fromSequenceNr)
        {
            var items = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && fromSequenceNr <= x.SequenceNumber)
                .ToList();

            long highestSequenceNumber = 0L;
            if (items != null && items.Any())
            {
                highestSequenceNumber = items.Max(x => x.SequenceNumber);
            }

            APITrace.Warning("GetHighestSequenceNumber {0}", highestSequenceNumber);

            return highestSequenceNumber;
        }

        [DbAPIOperation]
        public List<JournalItemDto> GetJournalItemsRange(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            var journalItems = objectModel.GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && fromSequenceNr <= x.SequenceNumber && x.SequenceNumber <= toSequenceNr && !x.IsDeleted && !x.IsSoftDeleted)
                .OrderBy(x => x.SequenceNumber)
                .Select(x => new JournalItemDto
                {
                    Id = x.Id,
                    GroupKey = x.GroupKey,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    DocumentType = x.DocumentType,
                    Manifest = x.Manifest,
                    WriterGuid = x.WriterGuid,
                    Timestamp = x.Timestamp,
                    Type = x.Type,
                    Payload = x.Payload?.ToArray() ?? Array.Empty<byte>(),
                    Tag = x.Tag,
                    HighestSequenceNumber = x.HighestSequenceNumber,
                    IsSoftDeleted = x.IsSoftDeleted || x.IsDeleted
                })
                .ToList();

            return journalItems ?? new List<JournalItemDto>();
        }

        [DbAPIOperation]
        public void DeleteJournalItemsTo(ObjectModel objectModel, string persistenceId, long toSequenceNr)
        {
            var journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber <= toSequenceNr);

            int deleteCount = 0;
            foreach (var journalItem in journalItems)
            {
                journalItem.IsSoftDeleted = true;
                //journalItem.Delete();
                deleteCount++;
            }

            objectModel.ApplyChanges();

            journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.PersistenceId == persistenceId && x.SequenceNumber <= toSequenceNr && !x.IsDeleted && !x.IsSoftDeleted);

            APITrace.Warning("DeleteJournalItemsTo {0}, ItemCount: {1}, DeletedCount: {2}", persistenceId, journalItems.Count(), deleteCount);
        }

        [DbAPIOperation]
        public void UpdateJournalItem(ObjectModel objectModel, string persistenceId, JournalItemDto journalItemDto)
        {
            var journalItem = objectModel
                .GetAllObjects<JournalItem>()
                .FirstOrDefault(x => x.PersistenceId == persistenceId);

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
            journalItem.Payload = DatabaseArray<byte>.Create(journalItemDto.Payload ?? Array.Empty<byte>());
            journalItem.Tag = journalItemDto.Tag;
            journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;
            journalItem.IsSoftDeleted = journalItemDto.IsSoftDeleted;

            objectModel.ApplyChanges();

            APITrace.Warning("UpdateJournalItem - Updated {0}", persistenceId);
        }

        [DbAPIOperation]
        public List<JournalItemDto> GetTaggedJournalItems(ObjectModel objectModel, string tag, long fromOffset, long toOffset)
        {
            var tags = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.Tag == tag && fromOffset <= x.Timestamp && x.Timestamp <= toOffset && !x.IsSoftDeleted)
                .OrderBy(x => x.Timestamp)
                .Select(x => $"tag-{x.Tag}-{x.PersistenceId}")
                .ToList();

            var journalItems = objectModel.GetAllObjects<JournalItem>()
                .Where(x => !x.IsSoftDeleted)
                .Join(tags, x => x.GroupKey, y => y, (x, _) => x)
                .Select(x => new JournalItemDto
                {
                    Id = x.Id,
                    GroupKey = x.GroupKey,
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    DocumentType = x.DocumentType,
                    Manifest = x.Manifest,
                    WriterGuid = x.WriterGuid,
                    Timestamp = x.Timestamp,
                    Type = x.Type,
                    Payload = x.Payload?.ToArray() ?? Array.Empty<byte>(),
                    Tag = x.Tag,
                    HighestSequenceNumber = x.HighestSequenceNumber,
                    IsSoftDeleted = x.IsSoftDeleted || x.IsDeleted
                });

            return journalItems.ToList();
        }

        [DbAPIOperation]
        public List<string> GetPersistenceIds(ObjectModel objectModel)
        {
            var highestSequenceNumberPropertyName = nameof(JournalItem.HighestSequenceNumber);

            var persistenceIds = objectModel.GetAllObjects<JournalItem>()
                .Where(x => x.DocumentType == highestSequenceNumberPropertyName && !string.IsNullOrEmpty(x.PersistenceId) && !x.IsSoftDeleted)
                .Select(x => x.PersistenceId)
                .ToList();

            return persistenceIds;
        }

        [DbAPIOperation]
        public void Flush(ObjectModel objectModel)
        {
            var journalItems = objectModel.GetAllObjects<JournalItem>();
            foreach (var journalItem in journalItems)
            {
                journalItem.IsSoftDeleted = true;
                journalItem.Delete();
            }

            objectModel.ApplyChanges();
        }
    }
}