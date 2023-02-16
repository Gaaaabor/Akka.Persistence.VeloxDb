using Akka.Actor;
using Akka.Persistence.VeloxDb.Journal;
using Akka.Persistence.VeloxDb.Snapshot;

namespace Akka.Persistence.VeloxDb
{
    /// <summary>
    /// Singleton class for the VeloxDB for akka persistence plugin.
    /// </summary>
    public class VeloxDbPersistenceProvider : ExtensionIdProvider<VeloxDbPersistence>
    {
        /// <summary>
        /// Creates an actorsystem extension for VeloxDb akka persistence support.
        /// </summary>
        /// <param name="system"></param>
        /// <returns></returns>
        public override VeloxDbPersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(VeloxDbPersistence.DefaultConfig);

            var journalSettings = VeloxDbJournalSettings.Create(system.Settings.Config.GetConfig(VeloxDbJournalSettings.JournalConfigPath));
            var snapshotSettings = VeloxDbSnapshotStoreSettings.Create(system.Settings.Config.GetConfig(VeloxDbSnapshotStoreSettings.SnapshotStoreConfigPath));

            return new VeloxDbPersistence(journalSettings, snapshotSettings);
        }
    }
}
