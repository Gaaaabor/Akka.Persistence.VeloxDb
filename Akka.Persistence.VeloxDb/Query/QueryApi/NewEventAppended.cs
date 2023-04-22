using System;
using Akka.Event;

namespace Akka.Persistence.VeloxDb.Query.QueryApi
{
    [Serializable]
    public record NewEventAppended : IDeadLetterSuppression
    {
        public static NewEventAppended Instance = new();

        private NewEventAppended() { }
    }
}