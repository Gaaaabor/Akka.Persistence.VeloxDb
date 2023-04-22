using System;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    [Serializable]
    public record SubscribeNewEvents : ISubscriptionCommand
    {
        public static SubscribeNewEvents Instance = new();

        private SubscribeNewEvents() { }
    }
}