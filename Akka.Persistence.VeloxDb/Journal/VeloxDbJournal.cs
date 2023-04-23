using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.VeloxDb.Query.QueryApi;
using Akka.Util.Internal;
using System.Collections.Immutable;

namespace Akka.Persistence.VeloxDb.Journal
{
    public class VeloxDbJournal : AsyncWriteJournal, IWithUnboundedStash
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
        private IJournalItemApi _journalApi;
        private readonly VeloxDbJournalSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new();

        public VeloxDbJournal(Config? config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                VeloxDbPersistence.Get(Context.System).JournalSettings :
                VeloxDbJournalSettings.Create(config);

            _journalApi = VeloxDbSetup.InitJournalItemApi(_settings);
        }

        public IStash? Stash { get; set; }

        protected override void PreStart()
        {
            base.PreStart();
        }

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var returnedItems = 0L;

            var messages = _journalApi!.GetJournalItems(persistenceId, fromSequenceNr, toSequenceNr, _settings.ReplayMaxMessageCount);
            if (messages is null)
            {
                await Task.CompletedTask;
                return;
            }

            foreach (var message in messages)
            {
                if (returnedItems >= max)
                {
                    break;
                }
                
                recoveryCallback(new EventDocument(message).ToPersistent(_actorSystem));

                returnedItems++;
            }

            if (returnedItems == 0)
            {
                NotifyNewPersistenceIdAdded(persistenceId);
            }

            await Task.CompletedTask;
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            long highestSequenceNumber = _journalApi!.GetHighestSequenceNumber(persistenceId, fromSequenceNr);
            if (highestSequenceNumber <= 0)
            {
                NotifyNewPersistenceIdAdded(persistenceId);
            }

            return await Task.FromResult(highestSequenceNumber);
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var results = new List<Exception>();
            var allTags = new List<string>();

            foreach (var atomicWrite in messages)
            {
                try
                {
                    var items = atomicWrite.Payload.AsInstanceOf<IImmutableList<IPersistentRepresentation>>();

                    foreach (var persistentRepresentation in items)
                    {
                        if (persistentRepresentation.SequenceNr == 0)
                        {
                            NotifyNewPersistenceIdAdded(persistentRepresentation.PersistenceId);
                        }

                        var (documents, tags) = EventDocument.ToDocument(persistentRepresentation, _actorSystem);

                        allTags.AddRange(tags);

                        foreach (var document in documents)
                        {
                            _journalApi.CreateJournalItem(document);
                        }
                    }

                    var highestSequenceNumbers = items
                        .GroupBy(x => x.PersistenceId)
                        .Select(x => EventDocument.ToHighestSequenceNumberDocument(
                            x.Key,
                            x.Select(y => y.SequenceNr).OrderByDescending(y => y).FirstOrDefault())
                        )
                        .ToImmutableList();

                    foreach (var highestSequenceNumber in highestSequenceNumbers)
                    {
                        _journalApi.UpdateJournalItem(highestSequenceNumber.Id, highestSequenceNumber);
                    }

                    results.Add(null);
                }
                catch (Exception exception)
                {
                    results.Add(exception);
                }
            }

            var documentTags = allTags.Distinct().ToImmutableList();

            if (_tagSubscribers.Any() && documentTags.Any())
            {
                foreach (var tag in documentTags)
                    NotifyTagChange(tag);
            }

            return await Task.FromResult(results.Any(x => x != null) ? results.ToImmutableList() : null);
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            _journalApi.DeleteJournalItemsTo(persistenceId, long.MinValue, toSequenceNr);
            await Task.CompletedTask;
        }

        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case ReplayTaggedMessages replay:
                    ReplayTaggedMessagesAsync(replay)
                        .PipeTo(replay.ReplyTo, success: h => replay.IsCatchup ? new TagCatchupFinished(h) : new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
                    return true;

                case SubscribeAllPersistenceIds:
                    AddAllPersistenceIdSubscriber(Sender).PipeTo(Sender);
                    Context.Watch(Sender);
                    return true;

                case SubscribeTag subscribe:
                    AddTagSubscriber(Sender, subscribe.Tag);
                    Context.Watch(Sender);
                    return true;

                case Terminated terminated:
                    RemoveSubscriber(terminated.ActorRef);
                    return true;

                default:
                    return false;
            }
        }

        private async Task<long> ReplayTaggedMessagesAsync(ReplayTaggedMessages replay)
        {
            if (replay.FromOffset >= replay.ToOffset)
            {
                return await Task.FromResult(0);
            }

            var maxOrdering = 0L;
            var journalItems = _journalApi.GetTaggedJournalItems(replay.Tag, replay.FromOffset + 1, replay.ToOffset, replay.Max);
            if (journalItems is null)
            {
                return await Task.FromResult(maxOrdering);
            }

            var replayedItems = 0L;
            foreach (var result in journalItems.Select(x => new EventDocument(x)).OrderBy(x => x.Timestamp))
            {
                if (replayedItems >= replay.Max)
                {
                    await Task.FromResult(maxOrdering);
                }

                _log.Debug("Sending replayed message: persistenceId:{0} - sequenceNr:{1}", result.PersistenceId, result.SequenceNumber);

                replay.ReplyTo.Tell(new ReplayedTaggedMessage(
                        result.ToPersistent(_actorSystem),
                        replay.Tag,
                        result.Timestamp),
                    ActorRefs.NoSender);

                maxOrdering = Math.Max(maxOrdering, result.Timestamp);

                replayedItems++;
            }

            return await Task.FromResult(maxOrdering);
        }

        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _tagSubscribers.Add(tag, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private void RemoveSubscriber(IActorRef subscriber)
        {
            var tagSubscriptions = _tagSubscribers.Values.Where(x => x.Contains(subscriber));
            foreach (var subscription in tagSubscriptions)
            {
                subscription.Remove(subscriber);
            }

            _allPersistenceIdSubscribers.Remove(subscriber);
        }

        private async Task AddAllPersistenceIdSubscriber(IActorRef subscriber)
        {
            lock (_allPersistenceIdSubscribers)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
            }

            var persistenceIds = _journalApi.GetPersistenceIds();
            subscriber.Tell(new CurrentPersistenceIdsChunk(persistenceIds.ToImmutableList(), LastChunk: true));
        }

        private void NotifyTagChange(string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscribers))
            {
                return;
            }

            var changed = new TaggedEventAppended(tag);

            foreach (var subscriber in subscribers)
            {
                subscriber.Tell(changed);
            }
        }

        private void NotifyNewPersistenceIdAdded(string persistenceId)
        {
            var added = new PersistenceIdAdded(persistenceId);

            foreach (var subscriber in _allPersistenceIdSubscribers)
            {
                subscriber.Tell(added);
            }
        }
    }
}
