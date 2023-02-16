using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Persistence.VeloxDb.Events;
using Akka.Persistence.VeloxDb.Extensions;
using System.Collections.Immutable;
using VeloxDB.Client;

namespace Akka.Persistence.VeloxDb.Journal
{
    public class VeloxDbJournal : AsyncWriteJournal, IWithUnboundedStash
    {
        private readonly ActorSystem _actorSystem;
        private readonly VeloxDbJournalSettings _veloxDbJournalSettings;
        private readonly ILoggingAdapter _logger = Context.GetLogger();

        private readonly IDictionary<string, ISet<IActorRef>> _tagSubscribers = new Dictionary<string, ISet<IActorRef>>();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new HashSet<IActorRef>();

        private IJournalApi _journalApi;

        public IStash Stash { get; set; }

        public VeloxDbJournal(Config? config = null)
        {
            _actorSystem = Context.System;

            _veloxDbJournalSettings = config is null
                ? VeloxDbPersistence.Get(Context.System).JournalSettings
                : VeloxDbJournalSettings.Create(config);

            var connectionStringParams = new ConnectionStringParams();
            connectionStringParams.AddAddress(_veloxDbJournalSettings.Address);
            //connectionStringParams.ServiceName

            _journalApi = ConnectionFactory.Get<IJournalApi>(connectionStringParams.GenerateConnectionString());
        }

        protected override void PreStart()
        {
            base.PreStart();

            //if (!_veloxDbJournalSettings.AutoInitialize)
            //{
            //    _journalApi = _journalApi.CreateTable();
            //    return;
            //}

            InitializeAsync().PipeTo(Self);
            BecomeStacked(WaitingForInitialization);
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            long highestSequenceNumber = await _journalApi!.GetHighestSequenceNumberAsync(persistenceId, fromSequenceNr);
            if (highestSequenceNumber <= 0)
            {
                NotifyNewPersistenceIdAdded(persistenceId);
            }

            return highestSequenceNumber;
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

            var messages = await _journalApi!.GetMessagesRangeAsync(persistenceId, fromSequenceNr, toSequenceNr, pageSize: 100);
            foreach (var message in messages)
            {
                if (returnedItems >= max)
                {
                    break;
                }

                recoveryCallback(message.ToPersistent(_actorSystem));

                returnedItems++;
            }

            if (returnedItems == 0)
            {
                NotifyNewPersistenceIdAdded(persistenceId);
            }
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            // TODO: Implement
            throw new NotImplementedException();
        }

        protected override async Task<IImmutableList<Exception?>?> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            // TODO: Implement
            throw new NotImplementedException();
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

        private async Task<object> InitializeAsync()
        {
            // TODO: Implement
            throw new NotImplementedException();
        }

        private bool WaitingForInitialization(object message)
        {
            return message
                .Match()
                .With<Initialized>(_ =>
                {
                    UnbecomeStacked();
                    Stash?.UnstashAll();
                })
                .With<Failure>(failure =>
                {
                    _logger.Error(failure.Exception, "Error during journal initialization");
                    Context.Stop(Self);
                })
                .Default(_ => Stash?.Stash())
                .WasHandled;
        }
    }
}
