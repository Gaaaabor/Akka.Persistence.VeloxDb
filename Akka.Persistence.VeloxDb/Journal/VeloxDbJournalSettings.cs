using Akka.Configuration;
using Akka.Persistence.VeloxDb;

namespace Akka.Persistence.VeloxDb.Journal
{
    public record VeloxDbJournalSettings(string Address, int ReplayMaxMessageCount) : IVeloxDbSettings
    {
        public const string JournalConfigPath = "akka.persistence.journal.veloxdb";

        public static VeloxDbJournalSettings Create(Config config)
        {
            var address = config.GetString("address");
            var replayMaxMessageCount = config.GetInt("replayMaxMessageCount", 1000);

            return new VeloxDbJournalSettings(address, replayMaxMessageCount);
        }
    }
}
