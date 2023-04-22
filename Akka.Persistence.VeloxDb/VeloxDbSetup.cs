using Akka.Persistence.VeloxDb.Journal;
using Akka.Persistence.VeloxDb.Snapshot;
using VeloxDB.Client;

namespace Akka.Persistence.VeloxDb
{
    public class VeloxDbSetup
    {
        public static IJournalItemApi InitJournalItemApi(IVeloxDbSettings settings)
        {
            var connectionStringParams = new ConnectionStringParams();
            connectionStringParams.AddAddress(settings.Address);

            var journalApi = ConnectionFactory.Get<IJournalItemApi>(connectionStringParams.GenerateConnectionString());
            return journalApi;
        }

        public static ISnapshotStoreApi InitSnapshotStoreApi(IVeloxDbSettings settings)
        {
            var connectionStringParams = new ConnectionStringParams();
            connectionStringParams.AddAddress(settings.Address);

            var snapshotStoreApi = ConnectionFactory.Get<ISnapshotStoreApi>(connectionStringParams.GenerateConnectionString());
            return snapshotStoreApi;
        }
    }
}