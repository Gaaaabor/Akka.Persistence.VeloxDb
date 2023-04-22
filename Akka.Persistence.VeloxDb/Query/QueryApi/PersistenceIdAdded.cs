using Akka.Event;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    public record PersistenceIdAdded(string PersistenceId) : IDeadLetterSuppression;
}