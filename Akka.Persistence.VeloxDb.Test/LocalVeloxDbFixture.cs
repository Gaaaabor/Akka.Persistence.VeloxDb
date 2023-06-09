using Akka.Persistence.VeloxDb.Journal;
using Akka.Persistence.VeloxDb.Snapshot;
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using VeloxDB.Client;

namespace Akka.Persistence.VeloxDb.Test
{
    public class LocalVeloxDbFixture : IDisposable
    {
        private readonly Process _process;

        public int ReplayMaxMessageCount { get; set; }
        public string Address { get; set; }

        public LocalVeloxDbFixture()
        {
            Address = "localhost:7568";
            ReplayMaxMessageCount = 1000;

            var assemblyName = Assembly.GetExecutingAssembly().GetName().Name;
            var index = Directory.GetCurrentDirectory().IndexOf(assemblyName);
            var pathBase = Directory.GetCurrentDirectory()[..index];
            var dbProjectPath = Path.Combine(pathBase, "Akka.Persistence.VeloxDb.Db");

            _process = new Process
            {
                StartInfo =
                {
                    FileName = "dotnet",
                    Arguments = "run",
                    UseShellExecute = true,
                    WorkingDirectory = dbProjectPath
                }
            };

            var isStarted =_process.Start();

            if (isStarted)
            {
                var connectionStringParams = new ConnectionStringParams();
                connectionStringParams.AddAddress(Address);
                var connectionString = connectionStringParams.GenerateConnectionString();

                var journalApi = ConnectionFactory.Get<IJournalItemApi>(connectionString);
                journalApi.Flush();

                var snapshotStoreApi = ConnectionFactory.Get<ISnapshotStoreItemApi>(connectionString);
                snapshotStoreApi.Flush(); 
            }
        }

        public void Dispose()
        {
            var connectionStringParams = new ConnectionStringParams();
            connectionStringParams.AddAddress(Address);
            var connectionString = connectionStringParams.GenerateConnectionString();

            var journalApi = ConnectionFactory.Get<IJournalItemApi>(connectionString);
            journalApi.Flush();

            var snapshotStoreApi = ConnectionFactory.Get<ISnapshotStoreItemApi>(connectionString);
            snapshotStoreApi.Flush();

            _process?.Kill(true);
            _process?.Dispose();
        }
    }
}