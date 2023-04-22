using Akka.Actor;
using Akka.Persistence.VeloxDb.Db;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Text.Json;

namespace Akka.Persistence.VeloxDb.Extensions
{
    public static class JournalItemExtensions
    {
        public static IPersistentRepresentation ToPersistent(this JournalItemDto journalItem, ActorSystem system)
        {
            var type = Type.GetType(journalItem.PayloadType, false) ?? typeof(object);

            object? payload = null;

            switch (journalItem.SerializationType)
            {
                case SerializationType.Json:
                    payload = JsonSerializer.Deserialize(journalItem.Payload, type);
                    break;

                case SerializationType.Bson:

                    var rawPayload = Convert.FromBase64String(journalItem.Payload);
                    using (MemoryStream memoryStream = new(rawPayload))
                    {
                        using BsonReader bsonReader = new(memoryStream);
                        var jObject = (JObject)JToken.ReadFrom(bsonReader);
                        payload = jObject.ToObject(type);
                    }
                    break;

                case SerializationType.Binary:
                    payload = system.Serialization
                        .FindSerializerFor(type)
                        .FromBinary(Encoding.UTF8.GetBytes(journalItem.Payload), type);
                    break;

                default:
                    payload = new object();
                    break;
            }

            return new Persistent(
                payload,
                journalItem.SequenceNumber,
                journalItem.PersistenceId,
                journalItem.Manifest,
                sender: ActorRefs.NoSender,
                writerGuid: journalItem.WriterGuid,
                timestamp: journalItem.Timestamp);
        }
    }
}
