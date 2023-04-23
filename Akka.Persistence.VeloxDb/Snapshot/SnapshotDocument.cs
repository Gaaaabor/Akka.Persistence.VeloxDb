using Akka.Actor;
using Akka.Persistence.VeloxDb.Db;
using System.Text;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public class SnapshotDocument
    {
        private readonly SnapshotStoreItemDto _snapshotItem;

        public SnapshotDocument(SnapshotStoreItemDto snapshotItem)
        {
            _snapshotItem = snapshotItem;
        }

        public string? PersistenceId => _snapshotItem.PersistenceId;

        public long SequenceNumber => _snapshotItem.SequenceNumber;

        public long Timestamp => _snapshotItem.Timestamp;

        public Type? Type => Type.GetType(_snapshotItem.Type ?? "System.Object");

        public SelectedSnapshot ToSelectedSnapshot(ActorSystem system)
        {
            object payload;
            if (Type == typeof(string))
            {
                payload = _snapshotItem.Payload;
            }
            else
            {
                var serializer = system.Serialization.FindSerializerForType(Type);
                var binaryPayload = Encoding.UTF8.GetBytes(_snapshotItem.Payload);
                payload = serializer.FromBinary(binaryPayload, Type);
            }

            return new SelectedSnapshot(new SnapshotMetadata(PersistenceId, SequenceNumber, new DateTime(Timestamp)), payload);
        }

        public static SnapshotStoreItemDto ToDocument(SnapshotMetadata metadata, object snapshot, ActorSystem system)
        {
            var type = snapshot.GetType();

            string payload;
            if (type == typeof(string))
            {
                payload = snapshot.ToString();
            }
            else
            {
                var serializer = system.Serialization.FindSerializerForType(type);
                var binaryPayload = serializer.ToBinary(snapshot);
                payload = Encoding.UTF8.GetString(binaryPayload);
            }

            return new SnapshotStoreItemDto
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