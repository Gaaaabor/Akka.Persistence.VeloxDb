using Akka.Persistence.TCK.Journal;

namespace Akka.Persistence.VeloxDb.Test.Journal
{
    public class VeloxDbJournalSpec : JournalSpec
    {
        public VeloxDbJournalSpec(LocalVeloxDbFixture fixture)
            : base(VeloxDbStorageConfigHelper.VeloxDbConfig(fixture))
        {
            VeloxDbPersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsSerialization { get; } = false;

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;
    }
}
