using Akka.Configuration;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public class VeloxDbSnapshotStoreSettings
    {
        public const string SnapshotStoreConfigPath = "akka.persistence.snapshot-store.veloxdb";

        public string? Address { get; private set; }

        public static VeloxDbSnapshotStoreSettings Create(Config config)
        {
            return new VeloxDbSnapshotStoreSettings
            {
                Address = config.GetString("address")
            };
        }
    }
}
