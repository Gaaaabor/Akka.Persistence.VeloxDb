namespace Akka.Persistence.VeloxDb.Test
{
    [CollectionDefinition(Name)]
    public class VeloxDbTestCollection : ICollectionFixture<LocalVeloxDbFixture>
    {
        public const string Name = "VeloxDb";
    }
}