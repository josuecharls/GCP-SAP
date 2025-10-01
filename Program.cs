using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Serilog;
using Serilog.Events;

namespace Sodimac.SAP.GCP
{
    internal static class Program
    {
        private const string SchemaName = "SAP";
        private static ILogger _logger = Log.Logger;

        private static readonly string[] SupportedTables =
        {
            "CapacitaWeb",
            "Permits",
            "Subsidios"
        };

        private static int Main(string[] args)
        {
            var exitCode = 0;

            try
            {
                ConfigureLogger();
                _logger.Information("Inicio del proceso de ingesta SAP");

                var connectionString = GetConnectionString("SAPDb");
                var inputFolder = GetRequiredSetting("InputFolder");
                var tempFolder = GetRequiredSetting("TempFolder");
                var bulkCopyTimeoutSeconds = GetOptionalIntSetting("BulkCopyTimeoutSeconds", 600);

                ProcessInputFolder(inputFolder, tempFolder, connectionString, bulkCopyTimeoutSeconds);

                _logger.Information("Proceso completado correctamente");
            }
            catch (Exception ex)
            {
                exitCode = 1;
                _logger.Fatal(ex, "Error no controlado durante la ingesta");
            }
            finally
            {
                Log.CloseAndFlush();
            }

            return exitCode;
        }

        private static void ConfigureLogger()
        {
            var logFilePath = GetRequiredSetting("LogFilePath");
            var logOutputTemplate = ConfigurationManager.AppSettings["LogOutputTemplate"] ??
                                     "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}";

            var directory = Path.GetDirectoryName(logFilePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .WriteTo.File(logFilePath,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: null,
                    shared: true,
                    outputTemplate: logOutputTemplate)
                .CreateLogger();

            Log.Logger = _logger;
        }

        private static void ProcessInputFolder(string inputFolder, string tempFolder, string connectionString, int bulkCopyTimeoutSeconds)
        {
            if (!Directory.Exists(inputFolder))
            {
                throw new DirectoryNotFoundException($"La carpeta de entrada configurada '{inputFolder}' no existe.");
            }

            Directory.CreateDirectory(tempFolder);

            var zipFiles = Directory.EnumerateFiles(inputFolder, "*.zip", SearchOption.TopDirectoryOnly).ToList();
            if (!zipFiles.Any())
            {
                _logger.Warning("No se encontraron archivos ZIP en {InputFolder}", inputFolder);
                return;
            }

            var truncatedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var zipFile in zipFiles)
            {
                try
                {
                    ProcessZipFile(zipFile, tempFolder, connectionString, bulkCopyTimeoutSeconds, truncatedTables);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Error procesando el archivo {ZipFile}", zipFile);
                }
            }
        }

        private static void ProcessZipFile(string zipFilePath, string tempFolder, string connectionString, int bulkCopyTimeoutSeconds, HashSet<string> truncatedTables)
        {
            _logger.Information("Procesando archivo ZIP {ZipFile}", zipFilePath);

            var extractionFolder = Path.Combine(tempFolder, Path.GetFileNameWithoutExtension(zipFilePath) + "_" + DateTime.UtcNow.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture));
            Directory.CreateDirectory(extractionFolder);

            try
            {
                ZipFile.ExtractToDirectory(zipFilePath, extractionFolder);
                var csvFiles = Directory.EnumerateFiles(extractionFolder, "*.csv", SearchOption.AllDirectories).ToList();

                if (!csvFiles.Any())
                {
                    _logger.Warning("El ZIP {ZipFile} no contiene archivos CSV", zipFilePath);
                    return;
                }

                foreach (var csvFile in csvFiles)
                {
                    try
                    {
                        ProcessCsvFile(csvFile, connectionString, bulkCopyTimeoutSeconds, truncatedTables);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex, "Error procesando el archivo CSV {CsvFile}", csvFile);
                    }
                }
            }
            finally
            {
                try
                {
                    if (Directory.Exists(extractionFolder))
                    {
                        Directory.Delete(extractionFolder, true);
                    }
                }
                catch (Exception cleanupEx)
                {
                    _logger.Warning(cleanupEx, "No se pudo eliminar la carpeta temporal {ExtractionFolder}", extractionFolder);
                }
            }
        }

        private static void ProcessCsvFile(string csvFilePath, string connectionString, int bulkCopyTimeoutSeconds, HashSet<string> truncatedTables)
        {
            var tableName = Path.GetFileNameWithoutExtension(csvFilePath);
            if (string.IsNullOrWhiteSpace(tableName))
            {
                _logger.Warning("No se pudo determinar el nombre de tabla para el archivo {CsvFile}", csvFilePath);
                return;
            }

            if (!SupportedTables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
            {
                _logger.Warning("El archivo {CsvFile} no corresponde a una tabla soportada del esquema SAP", csvFilePath);
                return;
            }

            _logger.Information("Leyendo archivo CSV {CsvFile} para la tabla {Table}", csvFilePath, tableName);

            var dataTable = LoadDataTableFromCsv(csvFilePath, tableName);
            var shouldTruncate = truncatedTables.Add(tableName);

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                if (shouldTruncate)
                {
                    TruncateTable(connection, tableName);
                }

                if (dataTable.Rows.Count == 0)
                {
                    _logger.Information("El archivo {CsvFile} no contiene registros para insertar", csvFilePath);
                    return;
                }

                BulkInsertData(connection, dataTable, tableName, bulkCopyTimeoutSeconds);
                _logger.Information("Se insertaron {RowCount} registros en [SAP].[{Table}] desde {CsvFile}", dataTable.Rows.Count, tableName, csvFilePath);
            }
        }

        private static DataTable LoadDataTableFromCsv(string csvFilePath, string tableName)
        {
            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                TrimOptions = TrimOptions.Trim,
                BadDataFound = context => _logger.Warning("Dato inválido en {CsvFile}: {RawRecord}", csvFilePath, context.RawRecord),
                MissingFieldFound = null
            };

            using (var reader = new StreamReader(csvFilePath, Encoding.UTF8, true))
            using (var csv = new CsvReader(reader, configuration))
            {
                if (!csv.Read() || !csv.ReadHeader())
                {
                    throw new InvalidOperationException($"El archivo {csvFilePath} no contiene cabeceras de columnas.");
                }

                var headers = csv.HeaderRecord ?? Array.Empty<string>();
                if (headers.Length == 0)
                {
                    throw new InvalidOperationException($"El archivo {csvFilePath} no define columnas.");
                }

                var normalizedHeaders = new List<string>();
                var duplicates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var dataTable = new DataTable(tableName);

                foreach (var header in headers)
                {
                    var columnName = (header ?? string.Empty).Trim();
                    if (string.IsNullOrEmpty(columnName))
                    {
                        throw new InvalidOperationException($"El archivo {csvFilePath} contiene columnas sin nombre.");
                    }

                    if (!duplicates.Add(columnName))
                    {
                        throw new InvalidOperationException($"El archivo {csvFilePath} contiene columnas duplicadas: {columnName}.");
                    }

                    dataTable.Columns.Add(columnName, typeof(string));
                    normalizedHeaders.Add(columnName);
                }

                while (csv.Read())
                {
                    var row = dataTable.NewRow();
                    foreach (var columnName in normalizedHeaders)
                    {
                        var value = csv.GetField(columnName);
                        row[columnName] = string.IsNullOrWhiteSpace(value) ? (object)DBNull.Value : value;
                    }

                    dataTable.Rows.Add(row);
                }

                return dataTable;
            }
        }

        private static void TruncateTable(SqlConnection connection, string tableName)
        {
            using (var command = new SqlCommand($"[{SchemaName}].[TruncateTable]", connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new SqlParameter("@TableName", SqlDbType.NVarChar, 128) { Value = tableName });
                command.CommandTimeout = connection.ConnectionTimeout;
                command.ExecuteNonQuery();
                _logger.Information("Se ejecutó el truncate de la tabla [SAP].[{Table}]", tableName);
            }
        }

        private static void BulkInsertData(SqlConnection connection, DataTable dataTable, string tableName, int bulkCopyTimeoutSeconds)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = $"[{SchemaName}].[{tableName}]";
                bulkCopy.BatchSize = Math.Min(Math.Max(dataTable.Rows.Count, 1), 5000);
                bulkCopy.BulkCopyTimeout = bulkCopyTimeoutSeconds;

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.WriteToServer(dataTable);
            }
        }

        private static string GetRequiredSetting(string key)
        {
            var value = ConfigurationManager.AppSettings[key];
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ConfigurationErrorsException($"El valor de configuración '{key}' no está definido en App.config.");
            }

            return value;
        }

        private static string GetConnectionString(string name)
        {
            var connectionString = ConfigurationManager.ConnectionStrings[name]?.ConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ConfigurationErrorsException($"La cadena de conexión '{name}' no está configurada en App.config.");
            }

            return connectionString;
        }

        private static int GetOptionalIntSetting(string key, int defaultValue)
        {
            var value = ConfigurationManager.AppSettings[key];
            if (string.IsNullOrWhiteSpace(value))
            {
                return defaultValue;
            }

            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : defaultValue;
        }
    }
}