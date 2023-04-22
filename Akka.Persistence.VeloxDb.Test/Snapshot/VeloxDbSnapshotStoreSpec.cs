using Akka.Persistence.TCK.Snapshot;

namespace Akka.Persistence.VeloxDb.Test.Snapshot
{
    [Collection(VeloxDbTestCollection.Name)]
    public class VeloxDbSnapshotStoreSpec : SnapshotStoreSpec
    {
        public VeloxDbSnapshotStoreSpec(LocalVeloxDbFixture fixture)
            : base(VeloxDbStorageConfigHelper.VeloxDbConfig(fixture))
        {
            VeloxDbPersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsSerialization { get; } = false;
    }
}