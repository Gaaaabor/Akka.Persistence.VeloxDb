using Akka.Configuration;
using System.Net;
using System.Net.Sockets;

namespace Akka.Persistence.VeloxDb.Test
{
    public static class VeloxDbStorageConfigHelper
    {
        public static int GetRandomUnusedPort()
        {
            var listener = new TcpListener(IPAddress.Any, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        public static Config VeloxDbConfig(LocalVeloxDbFixture fixture)
        {
            return ConfigurationFactory.ParseString(
                    @"
akka {
    loglevel = DEBUG
    log-config-on-start = off
    test.single-expect-default = 30s

    persistence {
        publish-plugin-commands = on

        journal {
            plugin = ""akka.persistence.journal.veloxdb""

            veloxdb {
                class = ""Akka.Persistence.VeloxDb.Journal.VeloxDbJournal, Akka.Persistence.VeloxDb""
                replayMaxMessageCount = """ + fixture.ReplayMaxMessageCount + @"""
                address = """ + fixture.Address + @"""

                event-adapters {
                    color-tagger = ""Akka.Persistence.TCK.Query.ColorFruitTagger, Akka.Persistence.TCK""
                }
                event-adapter-bindings = {
                    ""System.String"" = color-tagger
                }
            }
        }

        query {
            journal {
                veloxdb {
                    class = ""Akka.Persistence.VeloxDb.Query.VeloxDbReadJournalProvider, Akka.Persistence.VeloxDb""
                    write-plugin = ""akka.persistence.journal.veloxdb""
                    refresh-interval = 1s
		            max-buffer-size = 150
                }
            }
        }

        snapshot-store {
            plugin = ""akka.persistence.snapshot-store.veloxdb""
            
            veloxdb {
                class = ""Akka.Persistence.VeloxDb.Snapshot.VeloxDbSnapshotStore, Akka.Persistence.VeloxDb""
                replayMaxMessageCount = """ + fixture.ReplayMaxMessageCount + @"""
                address = """ + fixture.Address + @"""
            }
        }
    }
}");
        }

    }
}