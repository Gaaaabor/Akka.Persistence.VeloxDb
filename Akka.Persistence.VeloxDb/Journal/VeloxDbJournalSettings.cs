using Akka.Configuration;

namespace Akka.Persistence.VeloxDb.Journal
{
    public record VeloxDbJournalSettings : IVeloxDbJournalSettings
    {
        public const string JournalConfigPath = "akka.persistence.journal.veloxdb";

        public string Address { get; private set; }

        public int ReplayMaxMessageCount { get; private set; }

        public static VeloxDbJournalSettings Create(Config config)
        {
            return new VeloxDbJournalSettings
            {
                Address = config.GetString("address") ?? "localhost:7569",
                ReplayMaxMessageCount = config.GetInt("peplayMaxMessageCount", 1000),
            };
        }
    }
}
