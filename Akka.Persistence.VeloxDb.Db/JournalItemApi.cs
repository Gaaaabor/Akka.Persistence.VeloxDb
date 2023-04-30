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
            journalItem.DocumentType = journalItemDto.DocumentType;
            journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;
            journalItem.Manifest = journalItemDto.Manifest;
            journalItem.Payload = DatabaseArray<byte>.Create(journalItemDto.Payload ?? Array.Empty<byte>());
            journalItem.PersistenceId = journalItemDto.PersistenceId;
            journalItem.SequenceNumber = journalItemDto.SequenceNumber;
            journalItem.Tag = journalItemDto.Tag;
            journalItem.Timestamp = journalItemDto.Timestamp;
            journalItem.Type = journalItemDto.Type;
            journalItem.WriterGuid = journalItemDto.WriterGuid;

            objectModel.ApplyChanges();

            APITrace.Warning("CreateJournalItem PersistenceId: {0}, GroupKey: {1}", journalItem.PersistenceId, journalItem.GroupKey);
        }

        [DbAPIOperation]
        public void CreateJournalItemBatch(ObjectModel objectModel, List<JournalItemDto> journalItemDtos)
        {
            foreach (var journalItemDto in journalItemDtos)
            {
                var journalItem = objectModel.CreateObject<JournalItem>();

                journalItem.GroupKey = journalItemDto.GroupKey;
                journalItem.DocumentType = journalItemDto.DocumentType;
                journalItem.HighestSequenceNumber = journalItemDto.HighestSequenceNumber;
                journalItem.Manifest = journalItemDto.Manifest;
                journalItem.Payload = DatabaseArray<byte>.Create(journalItemDto.Payload ?? Array.Empty<byte>());
                journalItem.PersistenceId = journalItemDto.PersistenceId;
                journalItem.SequenceNumber = journalItemDto.SequenceNumber;
                journalItem.Tag = journalItemDto.Tag;
                journalItem.Timestamp = journalItemDto.Timestamp;
                journalItem.Type = journalItemDto.Type;
                journalItem.WriterGuid = journalItemDto.WriterGuid;

                APITrace.Warning("CreateJournalItem PersistenceId: {0}, GroupKey: {1}", journalItem.PersistenceId, journalItem.GroupKey);
            }

            objectModel.ApplyChanges();
        }

        [DbAPIOperation]
        public long GetHighestSequenceNumber(ObjectModel objectModel, string persistenceId, long fromSequenceNr)
        {
            var items = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x =>
                    x.PersistenceId == persistenceId &&
                    fromSequenceNr <= x.SequenceNumber)
                .ToList();

            long highestSequenceNumber = 0L;
            if (items != null && items.Any())
            {
                highestSequenceNumber = items.Max(x => x.HighestSequenceNumber);
            }

            APITrace.Warning("GetHighestSequenceNumber {0}", highestSequenceNumber);

            return highestSequenceNumber;
        }

        [DbAPIOperation]
        public List<JournalItemDto> GetJournalItemsRange(ObjectModel objectModel, string persistenceId, long fromSequenceNr, long toSequenceNr, string groupKey)
        {
            var journalItems = objectModel.GetAllObjects<JournalItem>()
                .Where(x =>
                    x.GroupKey == groupKey &&
                    x.PersistenceId == persistenceId &&
                    fromSequenceNr <= x.SequenceNumber &&
                    x.SequenceNumber <= toSequenceNr &&
                    x.DocumentType == "Event" &&
                    !x.IsDeleted &&
                    !x.IsSoftDeleted)
                .Select(x => new JournalItemDto
                {
                    Id = x.Id,
                    DocumentType = x.DocumentType,
                    GroupKey = x.GroupKey,
                    HighestSequenceNumber = x.HighestSequenceNumber,
                    IsSoftDeleted = x.IsSoftDeleted || x.IsDeleted,
                    Manifest = x.Manifest,
                    Payload = x.Payload?.ToArray() ?? Array.Empty<byte>(),
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    Tag = x.Tag,
                    Timestamp = x.Timestamp,
                    Type = x.Type,
                    WriterGuid = x.WriterGuid
                })
                .OrderBy(x => x.Timestamp)
                .ToList();

            return journalItems ?? new List<JournalItemDto>();
        }

        [DbAPIOperation]
        public void DeleteJournalItemsTo(ObjectModel objectModel, string persistenceId, long toSequenceNr)
        {
            var journalItems = objectModel
                .GetAllObjects<JournalItem>()
                .Where(x => x.DocumentType == "Event" && x.PersistenceId == persistenceId && x.SequenceNumber <= toSequenceNr);

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
            var journalItems = objectModel.GetAllObjects<JournalItem>()
                .Where(x =>
                    x.Tag == tag &&
                    fromOffset <= x.Timestamp &&
                    x.Timestamp <= toOffset &&
                    x.DocumentType == "TagRef" &&
                    !x.IsDeleted &&
                    !x.IsSoftDeleted)
                .Select(x => new JournalItemDto
                {
                    DocumentType = x.DocumentType,
                    GroupKey = x.GroupKey,
                    HighestSequenceNumber = x.HighestSequenceNumber,
                    Id = x.Id,
                    IsSoftDeleted = x.IsSoftDeleted || x.IsDeleted,
                    Manifest = x.Manifest,
                    Payload = x.Payload?.ToArray() ?? Array.Empty<byte>(),
                    PersistenceId = x.PersistenceId,
                    SequenceNumber = x.SequenceNumber,
                    Tag = x.Tag,
                    Timestamp = x.Timestamp,
                    Type = x.Type,
                    WriterGuid = x.WriterGuid
                })
                .OrderBy(x => x.Timestamp);

            return journalItems.ToList();
        }

        [DbAPIOperation]
        public List<string> GetPersistenceIds(ObjectModel objectModel)
        {
            var persistenceIds = objectModel.GetAllObjects<JournalItem>()
                .Where(x =>
                    x.DocumentType == "HighestSequenceNumber" &&
                    !string.IsNullOrEmpty(x.PersistenceId) &&
                    !x.IsDeleted &&
                    !x.IsSoftDeleted)
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