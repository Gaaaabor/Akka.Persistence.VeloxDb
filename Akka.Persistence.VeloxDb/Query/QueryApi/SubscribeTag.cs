using System;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    [Serializable]
    public record SubscribeTag(string Tag) : ISubscriptionCommand;
}