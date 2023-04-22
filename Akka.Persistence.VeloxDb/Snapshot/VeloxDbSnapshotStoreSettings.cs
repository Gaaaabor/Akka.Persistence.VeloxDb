using Akka.Configuration;
using Akka.Persistence.VeloxDb;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public record VeloxDbSnapshotStoreSettings(string Address, int ReplayMaxMessageCount) : IVeloxDbSettings
    {
        public const string SnapshotStoreConfigPath = "akka.persistence.snapshot-store.veloxdb";

        public static VeloxDbSnapshotStoreSettings Create(Config config)
        {
            var address = config.GetString("address");
            var replayMaxMessageCount = config.GetInt("replayMaxMessageCount", 1000);

            return new VeloxDbSnapshotStoreSettings(address, replayMaxMessageCount);
        }
    }
}
