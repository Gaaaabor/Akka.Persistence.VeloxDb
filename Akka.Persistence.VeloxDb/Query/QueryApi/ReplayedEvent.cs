using System;
using Akka.Actor;
using Akka.Event;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    [Serializable]
    public record ReplayedEvent(IPersistentRepresentation Persistent, long Offset) 
        : INoSerializationVerificationNeeded, IDeadLetterSuppression;
}