using Akka.Persistence.VeloxDb.Db;
using Akka.Persistence.VeloxDb.Journal;
using Newtonsoft.Json;
using System;
using VeloxDB.Client;

namespace Akka.Persistence.VeloxDb.Test
{
    public class JournalItemApiTests
    {
        //[Fact]
        public void Test1()
        {
            var connectionStringParams = new ConnectionStringParams();
            connectionStringParams.AddAddress("localhost:7568");            

            var journalApi = ConnectionFactory.Get<IJournalItemApi>(connectionStringParams.GenerateConnectionString());            

            var testClass = new TestClass { Id = 1, Name = "TestName" };

            var dto = new JournalItemDto
            {
                Ordering = 1,
                PersistenceId = "A",
                SequenceNumber = 1,
                HighestSequenceNumber = 1,
                Timestamp = DateTime.UtcNow.Ticks,
                Manifest = "Manifest",
                SerializationType = SerializationType.Json,
                Payload = JsonConvert.SerializeObject(testClass),
                PayloadType = "Akka.Persistence.VeloxDb.Test.TestClass",
                Tags = "T",
                WriterGuid = Guid.NewGuid().ToString()
            };

            var journalItemId = journalApi.CreateJournalItem(dto);
            Assert.Equal(dto.PersistenceId, journalItemId);

            var highestSequenceNumber = journalApi.GetHighestSequenceNumber(dto.PersistenceId, 1);
            Assert.Equal(dto.HighestSequenceNumber, highestSequenceNumber);

            var list = journalApi.GetMessagesRange(dto.PersistenceId, 1, 1, 100);

            journalApi.DeleteMessagesTo(dto.PersistenceId, 1);
        }

        public class TestClass
        {
            public int Id { get; set; }
            public string? Name { get; set; }
        }
    }
}