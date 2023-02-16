namespace Akka.Persistence.VeloxDb.Events
{
    public class PersistenceIdAdded
    {
        private string PersistenceId { get; }

        public PersistenceIdAdded(string persistenceId)
        {
            PersistenceId = persistenceId;
        }
    }
}
