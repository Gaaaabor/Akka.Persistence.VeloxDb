using Akka.Actor;
using Akka.Persistence.VeloxDb.Db;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public class SnapshotMapper
    {
        public static SelectedSnapshot ToSelectedSnapshot(SnapshotStoreItemDto snapshotStoreItemDto, ActorSystem system)
        {
            var type = Type.GetType(snapshotStoreItemDto.Type ?? "System.Object");
            var serializer = system.Serialization.FindSerializerForType(type);
            var payload = serializer.FromBinary(snapshotStoreItemDto.Payload, type);

            var snapshotMetadata = new SnapshotMetadata(snapshotStoreItemDto.PersistenceId, snapshotStoreItemDto.SequenceNumber, new DateTime(snapshotStoreItemDto.Timestamp));
            return new SelectedSnapshot(snapshotMetadata, payload);
        }

        public static SnapshotStoreItemDto ToSnapshotStoreItemDto(SnapshotMetadata metadata, object snapshot, ActorSystem system)
        {
            var type = snapshot.GetType();
            var serializer = system.Serialization.FindSerializerForType(type);
            var payload = serializer.ToBinary(snapshot);

            return new SnapshotStoreItemDto
            {
                PersistenceId = metadata.PersistenceId,
                SequenceNumber = metadata.SequenceNr,
                Timestamp = metadata.Timestamp.Ticks,
                Type = $"{type.FullName}, {type.Assembly.GetName().Name}",
                Payload = payload
            };
        }
    }
}