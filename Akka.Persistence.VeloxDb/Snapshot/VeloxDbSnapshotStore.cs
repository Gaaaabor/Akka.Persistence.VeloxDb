using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Snapshot;

namespace Akka.Persistence.VeloxDb.Snapshot
{
    public class VeloxDbSnapshotStore : SnapshotStore, IWithUnboundedStash
    {
        private readonly ActorSystem _actorSystem;
        private ISnapshotStoreItemApi _snapshotStoreItemApi;
        private readonly VeloxDbSnapshotStoreSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        public IStash Stash { get; set; }

        public VeloxDbSnapshotStore(Config config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                VeloxDbPersistence.Get(Context.System).SnapshotSettings :
                VeloxDbSnapshotStoreSettings.Create(config);

            _snapshotStoreItemApi = VeloxDbSetup.InitSnapshotStoreItemApi(_settings);
        }

        protected override bool ReceivePluginInternal(object message)
        {
            return base.ReceivePluginInternal(message);
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var fromTimestamp = (criteria.MinTimestamp ?? DateTime.MinValue).Ticks;
            var toTimestamp = criteria.MaxTimeStamp.Ticks;
            var snapshot = _snapshotStoreItemApi.GetLatestSnapshotItemRange(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr, fromTimestamp, toTimestamp);
            if (snapshot != null)
            {
                var result = SnapshotMapper.ToSelectedSnapshot(snapshot, _actorSystem);
                return await Task.FromResult(result);
            }

            return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            _snapshotStoreItemApi.CreateSnapshotItem(SnapshotMapper.ToSnapshotStoreItemDto(metadata, snapshot, _actorSystem));
            await Task.CompletedTask;
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            _snapshotStoreItemApi.DeleteSnapshotItem(metadata.PersistenceId, metadata.SequenceNr, metadata.Timestamp.Ticks);
            await Task.CompletedTask;
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var fromTimestamp = (criteria.MinTimestamp ?? DateTime.MinValue).Ticks;
            var toTimestamp = criteria.MaxTimeStamp.Ticks;
            _snapshotStoreItemApi.DeleteSnapshotItemsRange(persistenceId, criteria.MinSequenceNr, criteria.MaxSequenceNr, fromTimestamp, toTimestamp);
            await Task.CompletedTask;
        }
    }
}
