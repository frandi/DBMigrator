using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DBMigrator
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(IConfiguration configuration, ILogger<Worker> logger)
        {
            Configuration = configuration;
            _logger = logger;
        }

        public IConfiguration Configuration { get; }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

            var config = new MigratorConfig
            {
                SourceConnection = Configuration.GetConnectionString("SourceConnection"),
                DestinationConnection = Configuration.GetConnectionString("DestinationConnection"),
                BatchNumber = Configuration.GetValue<int>("BatchNumber"),
                Tables = Configuration.GetSection("Tables").AsEnumerable().Select(t => t.Value).Where(t => !string.IsNullOrEmpty(t))
            };

            var migrator = new Migrator(config, _logger);
            await migrator.Start(false, stoppingToken);

            _logger.LogInformation("Bye!");
        }
    }
}
