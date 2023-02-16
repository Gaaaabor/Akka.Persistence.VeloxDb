namespace Akka.Persistence.VeloxDb.Events
{
    public class TaggedEventAppended
    {
        public string Tag { get; }

        public TaggedEventAppended(string tag)
        {
            Tag = tag;
        }
    }
}
