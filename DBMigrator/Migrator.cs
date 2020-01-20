using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DBMigrator
{
    public class Migrator
    {
        private readonly string _sourceConnString;
        private readonly string _destinationConnString;
        private readonly int _batchNumber;
        private readonly IEnumerable<string> _tables;
        private readonly ILogger _logger;

        public Migrator(MigratorConfig config, ILogger logger)
        {
            _sourceConnString = config.SourceConnection;
            _destinationConnString = config.DestinationConnection;
            _tables = config.Tables;
            _batchNumber = config.BatchNumber;

            _logger = logger;
        }

        public async Task Start(bool runOnce = false, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Starting data migration..");

            foreach (var table in _tables.OrderBy(t => t))
            {
                if (!cancellationToken.IsCancellationRequested)
                    await ProcessTable(table, runOnce, cancellationToken);
            }
        }

        private async Task ProcessTable(string table, bool runOnce, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"=== Processing table [{table}].. ===");

            var currentDir = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var sqlFile = Path.Combine(currentDir, $"{table}-{DateTime.UtcNow.Ticks}.sql");

            var offset = 0;
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                var list = await GetData(table, _batchNumber, offset, cancellationToken);
                if (list.Any())
                {
                    await ImportDataToFile(table, list, sqlFile, cancellationToken);
                    await ImportDataToDatabase(table, list, cancellationToken);

                    if (runOnce)
                        break;
                }
                else
                {
                    break;
                }

                offset += _batchNumber;
            }
        }

        private async Task<List<Dictionary<string, object>>> GetData(string table, int num, int offset, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Fetching data with offset {offset} rows..");

            var list = new List<Dictionary<string, object>>();

            try
            {
                using (var conn = new SqlConnection(_sourceConnString))
                {
                    await conn.OpenAsync();

                    var sql = $"select * from {table} order by Id offset {offset} rows fetch next {num} rows only";
                    _logger.LogDebug(sql);

                    var command = new SqlCommand(sql, conn);
                    using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            if (cancellationToken.IsCancellationRequested)
                                break;

                            var dict = Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, reader.GetValue);
                            list.Add(dict);
                        }
                    }

                    await conn.CloseAsync();
                }

                if (list.Any())
                {
                    int firstId = (int)list.First().GetValueOrDefault("Id");
                    int lastId = (int)list.Last().GetValueOrDefault("Id");
                    _logger.LogInformation($"Fetch data with Id {firstId} - {lastId}");
                }
                else
                {
                    _logger.LogInformation("No data fetched.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }

            return list;
        }

        private async Task ImportDataToDatabase(string table, List<Dictionary<string, object>> list, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Importing {list.Count} rows of data..");

            var sql = new StringBuilder();
            sql.AppendLine($"SET IDENTITY_INSERT {table} ON;");

            foreach (var dict in list)
            {
                if (!cancellationToken.IsCancellationRequested)
                    sql.AppendLine(GenerateInsertStatement(table, dict));
            }

            try
            {
                using (var conn = new SqlConnection(_destinationConnString))
                {
                    await conn.OpenAsync();

                    var command = new SqlCommand(sql.ToString(), conn);
                    await command.ExecuteNonQueryAsync(cancellationToken);

                    await conn.CloseAsync();
                }

                _logger.LogInformation("Import done.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }
        }

        private async Task ImportDataToFile(string table, List<Dictionary<string, object>> list, string sqlFile, CancellationToken cancellationToken)
        {
            try
            {
                using (var writer = new StreamWriter(sqlFile, true))
                {
                    foreach (var dict in list)
                    {
                        if (!cancellationToken.IsCancellationRequested)
                            await writer.WriteLineAsync(GenerateInsertStatement(table, dict));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }
        }

        private string GenerateInsertStatement(string table, Dictionary<string, object> dict)
        {
            var columns = string.Join(",", dict.Keys.Select(k => $"[{k}]"));
            var values = string.Join(",", dict.Values.Select(v => 
                v != DBNull.Value ? $"'{v.ToString().Replace("'", "''")}'" : "null")
            );
            
            return $"insert into {table} ({columns}) values ({values});";
        }
    }
}
