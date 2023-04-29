using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.VeloxDb.Db;
using Akka.Persistence.VeloxDb.Query.QueryApi;
using Akka.Util.Internal;
using Newtonsoft.Json;
using System.Collections.Immutable;

namespace Akka.Persistence.VeloxDb.Journal
{
    public class VeloxDbJournal : AsyncWriteJournal, IWithUnboundedStash
    {
        private readonly ActorSystem _actorSystem;
        private IJournalItemApi _journalApi;
        private readonly VeloxDbJournalSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new();

        public IStash Stash { get; set; }

        public VeloxDbJournal(Config config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null ?
                VeloxDbPersistence.Get(Context.System).JournalSettings :
                VeloxDbJournalSettings.Create(config);

            _journalApi = VeloxDbSetup.InitJournalItemApi(_settings);
        }

        protected override void PreStart()
        {
            base.PreStart();
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
            var replayMessages = _journalApi.GetJournalItemsRange(persistenceId, fromSequenceNr, toSequenceNr);
            foreach (var replayMessage in replayMessages)
            {
                recoveryCallback(EventDocument.ToPersistent(replayMessage, _actorSystem));
                count++;

                if (count == max)
                {
                    return Task.CompletedTask;
                }
            }

            return Task.CompletedTask;
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            long highestSequenceNumber = _journalApi.GetHighestSequenceNumber(persistenceId, fromSequenceNr);
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
                    var groupedPersistentRepresentations = atomicWrite.Payload
                        .AsInstanceOf<IImmutableList<IPersistentRepresentation>>()
                        .GroupBy(x => x.PersistenceId)
                        .ToList();

                    ;

                    foreach (var groupedPersistentRepresentation in groupedPersistentRepresentations)
                    {
                        var journalItems = new List<JournalItemDto>();
                        foreach (var persistentRepresentation in groupedPersistentRepresentation)
                        {
                            var type = persistentRepresentation.Payload.GetType();
                            var serializer = _actorSystem.Serialization.FindSerializerForType(type);
                            var payload = serializer.ToBinary(persistentRepresentation.Payload);
                            var timestamp = persistentRepresentation.Timestamp > 0 ? persistentRepresentation.Timestamp : DateTime.UtcNow.Ticks;

                            _journalApi.CreateJournalItem(new JournalItemDto
                            {
                                GroupKey = EventDocument.GetEventGroupKey(persistentRepresentation.PersistenceId),
                                SequenceNumber = persistentRepresentation.SequenceNr,
                                PersistenceId = persistentRepresentation.PersistenceId,
                                Manifest = persistentRepresentation.Manifest,
                                WriterGuid = persistentRepresentation.WriterGuid,
                                Timestamp = timestamp,
                                Type = $"{type.FullName}, {type.Assembly.GetName().Name}",
                                Payload = payload,
                                DocumentType = EventDocument.DocumentTypes.Event,
                                HighestSequenceNumber = atomicWrite.HighestSequenceNr,
                                IsSoftDeleted = persistentRepresentation.IsDeleted
                            });

                            if (persistentRepresentation.SequenceNr == 0)
                            {
                                NotifyNewPersistenceIdAdded(persistentRepresentation.PersistenceId);
                            }

                            //if (persistentRepresentation.Payload is Tagged tagged)
                            //{
                            //    allTags.AddRange(tagged.Tags);

                            //    foreach (var tag in tagged.Tags)
                            //    {
                            //        _journalApi.CreateJournalItem(new JournalItemDto
                            //        {
                            //            GroupKey = EventDocument.GetTagGroupKey(tag, persistentRepresentation.PersistenceId),
                            //            SequenceNumber = persistentRepresentation.SequenceNr,
                            //            PersistenceId = persistentRepresentation.PersistenceId,
                            //            Timestamp = timestamp,
                            //            DocumentType = EventDocument.DocumentTypes.TagRef,
                            //            Tag = tag,
                            //            HighestSequenceNumber = atomicWrite.HighestSequenceNr,
                            //            Manifest = persistentRepresentation.Manifest,
                            //            Payload = payload,
                            //            Type = $"{type.FullName}, {type.Assembly.GetName().Name}",
                            //            WriterGuid = persistentRepresentation.WriterGuid,
                            //            IsSoftDeleted = persistentRepresentation.IsDeleted
                            //        });
                            //    }
                            //}
                        }

                        //_journalApi.CreateJournalItem(new JournalItemDto
                        //{
                        //    GroupKey = EventDocument.GetHighestSequenceNumberGroupKey(groupedPersistentRepresentation.Key),
                        //    SequenceNumber = 0L,
                        //    HighestSequenceNumber = atomicWrite.HighestSequenceNr,
                        //    DocumentType = EventDocument.DocumentTypes.HighestSequenceNumber,
                        //    PersistenceId = groupedPersistentRepresentation.Key,
                        //    IsSoftDeleted = groupedPersistentRepresentation.Any(x => x.IsDeleted)
                        //});
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
                {
                    NotifyTagChange(tag);
                }
            }

            return await Task.FromResult(results.Any(x => x != null) ? results.ToImmutableList() : null);
        }        

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            _journalApi.DeleteJournalItemsTo(persistenceId, toSequenceNr);

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
            var journalItems = _journalApi.GetTaggedJournalItems(replay.Tag, replay.FromOffset + 1, replay.ToOffset);
            if (journalItems is null || journalItems.Count == 0)
            {
                return await Task.FromResult(maxOrdering);
            }

            var replayedItems = 0L;
            foreach (var journalItem in journalItems)
            {
                if (replayedItems >= replay.Max)
                {
                    await Task.FromResult(maxOrdering);
                }

                _log.Debug("Sending replayed message: persistenceId:{0} - sequenceNr:{1}", journalItem.PersistenceId, journalItem.SequenceNumber);

                replay.ReplyTo.Tell(new ReplayedTaggedMessage(EventDocument.ToPersistent(journalItem, _actorSystem), replay.Tag, journalItem.Timestamp), ActorRefs.NoSender);

                maxOrdering = Math.Max(maxOrdering, journalItem.Timestamp);

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

            await Task.CompletedTask;
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
