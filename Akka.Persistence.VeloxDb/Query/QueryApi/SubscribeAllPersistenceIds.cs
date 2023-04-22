namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    public record SubscribeAllPersistenceIds : ISubscriptionCommand
    {
        public static readonly SubscribeAllPersistenceIds Instance = new();

        private SubscribeAllPersistenceIds()
        {
        }
    }
}