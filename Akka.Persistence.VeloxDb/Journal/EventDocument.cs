using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Persistence.VeloxDb.Db;
using System.Collections.Immutable;
using System.Text;

namespace Akka.Persistence.VeloxDb.Journal
{
    public class EventDocument
    {
        private readonly JournalItemDto _journalItem;

        public string? PersistenceId => _journalItem.PersistenceId;
        public long SequenceNumber => _journalItem.SequenceNumber;
        public string? Manifest => _journalItem.Manifest;
        public string? WriterGuid => _journalItem.WriterGuid;
        public long Timestamp => _journalItem.Timestamp;
        public long HighestSequenceNumber => _journalItem.HighestSequenceNumber;
        public Type? Type => Type.GetType(_journalItem.Type ?? "System.Object");

        public EventDocument(JournalItemDto journalItem)
        {
            _journalItem = journalItem;
        }

        public IPersistentRepresentation ToPersistent(ActorSystem system)
        {
            var serializer = system.Serialization.FindSerializerFor(Type);

            var binaryPayload = Encoding.UTF8.GetBytes(_journalItem.Payload); // TODO: Check!!!
            var payload = serializer.FromBinary(binaryPayload, Type);

            return new Persistent(
                payload ?? new object(),
                SequenceNumber,
                PersistenceId,
                Manifest,
                sender: ActorRefs.NoSender,
                writerGuid: WriterGuid,
                timestamp: Timestamp);
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
            var binaryPayload = serializer.ToBinary(item.Payload);
            var payload = Encoding.UTF8.GetString(binaryPayload); // TODO: Check!!!
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