using System.Collections.Immutable;
using Akka.Event;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    public record CurrentPersistenceIdsChunk(IImmutableList<string> PersistenceIds, bool LastChunk) 
        : IDeadLetterSuppression;
}