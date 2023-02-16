using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.VeloxDb.Journal;
using Akka.Persistence.VeloxDb.Snapshot;

namespace Akka.Persistence.VeloxDb
{
    public class VeloxDbPersistence : IExtension
    {
        public VeloxDbJournalSettings JournalSettings { get; private set; }
        public VeloxDbSnapshotStoreSettings SnapshotSettings { get; private set; }

        public static Config DefaultConfig => ConfigurationFactory.FromResource<VeloxDbPersistence>("Akka.Persistence.VeloxDb.reference.conf");

        public VeloxDbPersistence(VeloxDbJournalSettings journalSettings, VeloxDbSnapshotStoreSettings snapshotSettings)
        {
            JournalSettings = journalSettings;
            SnapshotSettings = snapshotSettings;
        }

        public static VeloxDbPersistence Get(ActorSystem system)
        {
            return system.WithExtension<VeloxDbPersistence, VeloxDbPersistenceProvider>();
        }
    }
}
