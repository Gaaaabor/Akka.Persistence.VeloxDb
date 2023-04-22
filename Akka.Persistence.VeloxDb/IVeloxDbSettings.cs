namespace Akka.Persistence.VeloxDb
{
    public interface IVeloxDbSettings
    {
        string Address { get; }
        int ReplayMaxMessageCount { get; }
    }
}