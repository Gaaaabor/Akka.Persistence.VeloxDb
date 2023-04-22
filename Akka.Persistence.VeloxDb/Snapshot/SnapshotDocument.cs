using Akka.Actor;
using Akka.Persistence.VeloxDb.Db;
using System.Text;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public class SnapshotDocument
    {
        private readonly SnapshotItemDto _snapshotItem;

        public SnapshotDocument(SnapshotItemDto snapshotItem)
        {
            _snapshotItem = snapshotItem;
        }

        public string? PersistenceId => _snapshotItem.PersistenceId;

        public long SequenceNumber => _snapshotItem.SequenceNumber;

        public long Timestamp => _snapshotItem.Timestamp;

        public Type? Type => Type.GetType(_snapshotItem.Type ?? "System.Object");

        public SelectedSnapshot ToSelectedSnapshot(ActorSystem system)
        {
            var serializer = system.Serialization.FindSerializerForType(Type);
            
            var binaryPayload = Encoding.UTF8.GetBytes(_snapshotItem.Payload); // TODO: Check!!!
            var payload = serializer.FromBinary(binaryPayload, Type);

            return new SelectedSnapshot(new SnapshotMetadata(PersistenceId, SequenceNumber, new DateTime(Timestamp)), payload);
        }

        public static SnapshotItemDto ToDocument(SnapshotMetadata metadata, object snapshot, ActorSystem system)
        {
            var type = snapshot.GetType();
            var serializer = system.Serialization.FindSerializerForType(type);
            var binaryPayload = serializer.ToBinary(snapshot);
            var payload = Encoding.UTF8.GetString(binaryPayload); // TODO: Check!!!

            return new SnapshotItemDto
            {
                PersistenceId = metadata.PersistenceId,
                SequenceNumber = metadata.SequenceNr,
                Timestamp = metadata.Timestamp.Ticks,
                Type = $"{type.FullName}, {type.Assembly.GetName().Name}",
                Payload = payload
            };
        }

        public static class Keys
        {
            public static string PersistenceId = nameof(PersistenceId);
            public static string SequenceNumber = nameof(SequenceNumber);
            public static string Timestamp = nameof(Timestamp);
            public static string Type = nameof(Type);
            public static string Payload = nameof(Payload);
        }
    }
}