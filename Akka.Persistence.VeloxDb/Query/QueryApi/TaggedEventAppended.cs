using System;
using Akka.Event;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    [Serializable]
    public record TaggedEventAppended(string Tag) : IDeadLetterSuppression;
}