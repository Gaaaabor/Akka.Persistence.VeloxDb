using System;
using System.Diagnostics;
using System.IO;

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

            string dbPath = Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.Parent.FullName, "Akka.Persistence.VeloxDb.Db");
            _process = new Process
            {
                StartInfo =
                {
                    FileName = "dotnet",
                    Arguments = "run",
                    UseShellExecute = false,
                    WorkingDirectory = dbPath
                }
            };

            _process.Start();
        }

        public void Dispose()
        {
            _process?.Kill();
            _process?.Dispose();
        }
    }
}