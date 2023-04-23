using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Snapshot;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public class VeloxDbSnapshotStore : SnapshotStore, IWithUnboundedStash
    {
        public static class Events
        {
            public sealed class Initialized
            {
                public static readonly Initialized Instance = new();
                private Initialized() { }
            }
        }

        private readonly ActorSystem _actorSystem;
        private ISnapshotStoreItemApi _snapshotStoreItemApi;
        private readonly VeloxDbSnapshotStoreSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        public VeloxDbSnapshotStore(Config? config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                VeloxDbPersistence.Get(Context.System).SnapshotSettings :
                VeloxDbSnapshotStoreSettings.Create(config);

            _snapshotStoreItemApi = VeloxDbSetup.InitSnapshotStoreItemApi(_settings);
        }

        protected override void PreStart()
        {
            base.PreStart();
        }

        public IStash? Stash { get; set; }

        protected override async Task<SelectedSnapshot?> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var fromTimestamp = (criteria.MinTimestamp ?? DateTime.MinValue).Ticks;
            var toTimestamp = criteria.MaxTimeStamp.Ticks;
            var snapshot = _snapshotStoreItemApi.GetLatestSnapshotItem(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr, fromTimestamp, toTimestamp);
            if (snapshot != null)
            {
                var result = new SnapshotDocument(snapshot).ToSelectedSnapshot(_actorSystem);
                return await Task.FromResult(result);
            }

            return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            _snapshotStoreItemApi.CreateSnapshotItem(SnapshotDocument.ToDocument(metadata, snapshot, _actorSystem));
            await Task.CompletedTask;
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            _snapshotStoreItemApi.DeleteSnapshotItemsTo(metadata.PersistenceId, 0, metadata.SequenceNr);
            await Task.CompletedTask;
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            _snapshotStoreItemApi.DeleteSnapshotItemsTo(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr);
            await Task.CompletedTask;
        }
    }
}
