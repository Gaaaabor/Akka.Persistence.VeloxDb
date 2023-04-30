using Akka.Actor;
using Akka.Persistence.VeloxDb.Db;

namespace Akka.Persistence.VeloxDb.Journal
{
    public class JournalMapper
    {
        public static IPersistentRepresentation ToPersistent(JournalItemDto journalItem, ActorSystem system)
        {
            var type = Type.GetType(journalItem.Type ?? "System.Object");
            var serializer = system.Serialization.FindSerializerFor(type);
            var rawPayload = journalItem.Payload != null ? journalItem.Payload.ToArray() : default;
            var payload = serializer.FromBinary(rawPayload, type) ?? new object();

            return new Persistent(
                payload: payload,
                sequenceNr: journalItem.SequenceNumber,
                persistenceId: journalItem.PersistenceId,
                manifest: journalItem.Manifest,
                isDeleted: journalItem.IsSoftDeleted,                
                sender: ActorRefs.NoSender,
                writerGuid: journalItem.WriterGuid,
                timestamp: journalItem.Timestamp);
        }

        public static JournalItemDto ToHighestSequenceNumberDocument(string persistenceId, long highestSequenceNumber)
        {
            return new JournalItemDto
            {
                DocumentType = DocumentTypes.HighestSequenceNumber,
                GroupKey = GetHighestSequenceNumberGroupKey(persistenceId),
                HighestSequenceNumber = highestSequenceNumber,
                PersistenceId = persistenceId,
                SequenceNumber = 0L
            };
        }

        public static string GetEventGroupKey(string persistenceId) => $"event-{persistenceId}";

        public static string GetTagGroupKey(string tag, string persistenceId) => $"tag-{tag}-{persistenceId}";

        public static string GetHighestSequenceNumberGroupKey(string persistenceId) => $"highestsequencenumber-{persistenceId}";

        public static class DocumentTypes
        {
            public const string Event = nameof(Event);
            public const string TagRef = nameof(TagRef);
            public const string HighestSequenceNumber = nameof(HighestSequenceNumber);
        }
    }
}