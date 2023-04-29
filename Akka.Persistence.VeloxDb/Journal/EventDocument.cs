using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Persistence.VeloxDb.Db;
using System.Collections.Immutable;

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

        public static (IImmutableList<JournalItemDto> docs, IImmutableList<string> tags) ToDocument(IPersistentRepresentation persistentRepresentation, ActorSystem system)
        {
            var item = persistentRepresentation;

            var tags = new List<string>();

            if (item.Payload is Tagged tagged)
            {
                item = item.WithPayload(tagged.Payload);
                tags.AddRange(tagged.Tags);
            }

            var type = item.Payload.GetType();
            var serializer = system.Serialization.FindSerializerForType(type);
            var payload = serializer.ToBinary(item.Payload);
            var timestamp = item.Timestamp > 0 ? item.Timestamp : DateTime.UtcNow.Ticks;

            var docs = new List<JournalItemDto>
            {
                new JournalItemDto
                {
                    GroupKey = GetEventGroupKey(item.PersistenceId),
                    SequenceNumber = item.SequenceNr,
                    PersistenceId = item.PersistenceId,
                    Manifest = item.Manifest,
                    WriterGuid = item.WriterGuid,
                    Timestamp = timestamp,
                    Type = $"{type.FullName}, {type.Assembly.GetName().Name}",
                    Payload = payload,
                    DocumentType = DocumentTypes.Event
                }
            };

            docs.AddRange(tags.Select(tag => new JournalItemDto
            {
                GroupKey = GetTagGroupKey(tag, item.PersistenceId),
                SequenceNumber = item.SequenceNr,
                PersistenceId = item.PersistenceId,
                Timestamp = timestamp,
                DocumentType = DocumentTypes.TagRef,
                Tag = tag
            }));

            return (docs.ToImmutableList(), tags.ToImmutableList());
        }

        public static JournalItemDto ToHighestSequenceNumberDocument(string persistenceId, long highestSequenceNumber)
        {
            return new JournalItemDto
            {
                GroupKey = GetHighestSequenceNumberGroupKey(persistenceId),
                SequenceNumber = 0L,
                HighestSequenceNumber = highestSequenceNumber,
                DocumentType = DocumentTypes.HighestSequenceNumber,
                PersistenceId = persistenceId
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