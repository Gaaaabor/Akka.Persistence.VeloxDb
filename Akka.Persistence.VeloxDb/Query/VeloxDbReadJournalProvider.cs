using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Query;

namespace Akka.Persistence.VeloxDb.Query
{
    public class VeloxDbReadJournalProvider : IReadJournalProvider
    {
        private readonly Config _config;

        public VeloxDbReadJournalProvider(ExtendedActorSystem system, Config config)
        {
            _config = config;
        }

        public IReadJournal GetReadJournal()
        {
            return new VeloxDbReadJournal(_config);
        }
    }
}