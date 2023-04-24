using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;

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
            var pathBase = Directory.GetCurrentDirectory().Substring(0, index);
            var dbProjectPath = Path.Combine(pathBase, "Akka.Persistence.VeloxDb.Db");

            _process = new Process
            {
                StartInfo =
                {
                    FileName = "dotnet",
                    Arguments = "run",
                    UseShellExecute = false,
                    WorkingDirectory = dbProjectPath
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