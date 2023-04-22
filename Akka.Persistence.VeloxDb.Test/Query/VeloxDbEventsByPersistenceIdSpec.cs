using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Persistence.VeloxDb.Query;

namespace Akka.Persistence.VeloxDb.Test.Query
{
    [Collection(VeloxDbTestCollection.Name)]
    public class VeloxDbEventsByPersistenceIdSpec : EventsByPersistenceIdSpec
    {
        public VeloxDbEventsByPersistenceIdSpec(LocalVeloxDbFixture fixture)
            : base(VeloxDbStorageConfigHelper.VeloxDbConfig(fixture))
        {
            VeloxDbPersistence.Get(Sys);
            
            ReadJournal = Sys.ReadJournalFor<VeloxDbReadJournal>(VeloxDbReadJournal.Identifier);
        }
    }
}