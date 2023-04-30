using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.VeloxDb.Db;
using Akka.Persistence.VeloxDb.Query.QueryApi;
using Akka.Util.Internal;
using System.Collections.Immutable;

namespace Akka.Persistence.VeloxDb.Journal
{
    public class VeloxDbJournal : AsyncWriteJournal
    {
        private readonly ActorSystem _actorSystem;
        private IJournalItemApi _journalApi;
        private readonly VeloxDbJournalSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new();
        private readonly HashSet<IActorRef> _newEventsSubscriber = new();

        public VeloxDbJournal(Config config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                VeloxDbPersistence.Get(Context.System).JournalSettings :
                VeloxDbJournalSettings.Create(config);

            _journalApi = VeloxDbSetup.InitJournalItemApi(_settings);
        }

        public override Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            if (max <= 0 || (toSequenceNr - fromSequenceNr) < 0)
            {
                return Task.CompletedTask;
            }

            long count = 0;
            var replayMessages = _journalApi.GetJournalItemsRange(persistenceId, fromSequenceNr, toSequenceNr, JournalMapper.GetEventGroupKey(persistenceId));
            foreach (var replayMessage in replayMessages)
            {
                recoveryCallback(JournalMapper.ToPersistent(replayMessage, _actorSystem));
                count++;

                if (count == max)
                {
                    return Task.CompletedTask;
                }
            }

            return Task.CompletedTask;
        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            long highestSequenceNumber = _journalApi.GetHighestSequenceNumber(persistenceId, fromSequenceNr);
            if (highestSequenceNumber <= 0)
            {
                NotifyNewPersistenceIdAdded(persistenceId);
            }

            return Task.FromResult(highestSequenceNumber);
        }

        protected override Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var results = new List<Exception>();
            var allTags = new List<string>();

            foreach (var atomicWrite in messages)
            {
                try
                {
                    var persistentRepresentations = atomicWrite.Payload.AsInstanceOf<IImmutableList<IPersistentRepresentation>>();

                    var journalItems = new List<JournalItemDto>();
                    foreach (var persistentRepresentation in persistentRepresentations)
                    {
                        if (persistentRepresentation.SequenceNr == 0)
                        {
                            NotifyNewPersistenceIdAdded(persistentRepresentation.PersistenceId);
                        }

                        var timestamp = persistentRepresentation.Timestamp > 0 ? persistentRepresentation.Timestamp : DateTime.UtcNow.Ticks;

                        if (persistentRepresentation.Payload is Tagged tagged)
                        {
                            var item = persistentRepresentation.WithPayload(tagged.Payload);
                            allTags.AddRange(tagged.Tags);

                            var type = item.Payload.GetType();
                            var serializer = _actorSystem.Serialization.FindSerializerForType(type);
                            var payload = serializer.ToBinary(item.Payload);
                            var typeName = $"{type.FullName}, {type.Assembly.GetName().Name}";

                            foreach (var tag in tagged.Tags)
                            {
                                journalItems.Add(new JournalItemDto
                                {
                                    DocumentType = JournalMapper.DocumentTypes.TagRef,
                                    GroupKey = JournalMapper.GetTagGroupKey(tag, persistentRepresentation.PersistenceId),
                                    HighestSequenceNumber = atomicWrite.HighestSequenceNr,
                                    IsSoftDeleted = persistentRepresentation.IsDeleted,
                                    Manifest = persistentRepresentation.Manifest,
                                    Payload = payload,
                                    PersistenceId = persistentRepresentation.PersistenceId,
                                    SequenceNumber = persistentRepresentation.SequenceNr,
                                    Tag = tag,
                                    Timestamp = timestamp,
                                    Type = typeName,
                                    WriterGuid = persistentRepresentation.WriterGuid
                                });
                            }
                        }
                        else
                        {
                            var type = persistentRepresentation.Payload.GetType();
                            var serializer = _actorSystem.Serialization.FindSerializerForType(type);
                            var payload = serializer.ToBinary(persistentRepresentation.Payload);
                            var typeName = $"{type.FullName}, {type.Assembly.GetName().Name}";

                            journalItems.Add(new JournalItemDto
                            {
                                DocumentType = JournalMapper.DocumentTypes.Event,
                                HighestSequenceNumber = atomicWrite.HighestSequenceNr,
                                IsSoftDeleted = persistentRepresentation.IsDeleted,
                                Manifest = persistentRepresentation.Manifest,
                                Payload = payload,
                                PersistenceId = persistentRepresentation.PersistenceId,
                                SequenceNumber = persistentRepresentation.SequenceNr,
                                Timestamp = timestamp,
                                Type = typeName,
                                WriterGuid = persistentRepresentation.WriterGuid,
                                GroupKey = JournalMapper.GetEventGroupKey(persistentRepresentation.PersistenceId)
                            });
                        }
                    }

                    journalItems.Add(JournalMapper.ToHighestSequenceNumberDocument(atomicWrite.PersistenceId, atomicWrite.HighestSequenceNr));

                    _journalApi.CreateJournalItemBatch(journalItems);

                    results.Add(null);
                }
                catch (Exception exception)
                {
                    results.Add(exception);
                }
            }

            if (_newEventsSubscriber.Count != 0)
            {
                foreach (var subscriber in _newEventsSubscriber)
                {
                    subscriber.Tell(NewEventAppended.Instance);
                }
            }

            var documentTags = allTags.Distinct().ToImmutableList();
            if (_tagSubscribers.Any() && documentTags.Any())
            {
                foreach (var tag in documentTags)
                {
                    NotifyTagChange(tag);
                }
            }

            return Task.FromResult<IImmutableList<Exception>>(results.Any(x => x != null) ? results.ToImmutableList() : null);
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            _journalApi.DeleteJournalItemsTo(persistenceId, toSequenceNr);
            return Task.CompletedTask;
        }

        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case SubscribeNewEvents _:
                    _newEventsSubscriber.Add(Sender);
                    Context.Watch(Sender);
                    return true;

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

        private Task<long> ReplayTaggedMessagesAsync(ReplayTaggedMessages replay)
        {
            if (replay.FromOffset >= replay.ToOffset)
            {
                return Task.FromResult(0L);
            }

            var maxOrdering = 0L;
            var journalItems = _journalApi.GetTaggedJournalItems(replay.Tag, replay.FromOffset + 1, replay.ToOffset);
            if (journalItems is null || journalItems.Count == 0)
            {
                return Task.FromResult(maxOrdering);
            }

            var replayedItems = 0L;
            foreach (var journalItem in journalItems)
            {
                if (replayedItems >= replay.Max)
                {
                    return Task.FromResult(maxOrdering);
                }

                _log.Debug("Sending replayed message: persistenceId:{0} - sequenceNr:{1}", journalItem.PersistenceId, journalItem.SequenceNumber);

                replay.ReplyTo.Tell(
                    new ReplayedTaggedMessage(
                        JournalMapper.ToPersistent(journalItem, _actorSystem),
                        replay.Tag,
                        journalItem.Timestamp),
                    ActorRefs.NoSender);

                maxOrdering = Math.Max(maxOrdering, journalItem.Timestamp);

                replayedItems++;
            }

            return Task.FromResult(maxOrdering);
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

        private Task AddAllPersistenceIdSubscriber(IActorRef subscriber)
        {
            lock (_allPersistenceIdSubscribers)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
            }

            var persistenceIds = _journalApi.GetPersistenceIds();
            subscriber.Tell(new CurrentPersistenceIdsChunk(persistenceIds.ToImmutableList(), LastChunk: true));

            return Task.CompletedTask;
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
