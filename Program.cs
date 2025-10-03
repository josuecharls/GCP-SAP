using System;
using System.Configuration;
using System.Data;
using System.IO;
using Serilog;

namespace Sodimac.SAP.GCP
{
    internal class Program
    {
        private static ILogger _log;

        static int Main(string[] args)
        {
            _log = LoggerConfig.Create();
            _log.Information("==== Sodimac.SAP.GCP - Inicio ====");

            try
            {
                var input = ConfigurationManager.AppSettings["InputFolder"];
                var capPat = ConfigurationManager.AppSettings["CapacitaPattern"];
                var subPat = ConfigurationManager.AppSettings["SubsidiosPattern"];
                var perPat = ConfigurationManager.AppSettings["PermitsPattern"];
                var cs = ConfigurationManager.ConnectionStrings["SapDb"].ConnectionString;

                Directory.CreateDirectory(input);
                var loader = new BulkLoaderStrict(cs, _log);

                foreach (var csv in Directory.GetFiles(input, "*.csv"))
                    ProcessCsv(csv, loader, capPat, subPat, perPat);

                _log.Information("==== Sodimac.SAP.GCP - Fin OK ====");
                return 0;
            }
            catch (Exception ex)
            {
                _log?.Error(ex, "Error fatal");
                return 1;
            }
            finally
            {
                Serilog.Log.CloseAndFlush();
            }
        }

        private static void ProcessCsv(string csvPath, BulkLoaderStrict loader, string capPat, string subPat, string perPat)
        {
            var file = Path.GetFileName(csvPath) ?? "";
            var schema = "SAP";
            string table;

            if (Like(file, capPat)) table = "CapacitaWeb";
            else if (Like(file, subPat)) table = "Subsidios";
            else if (Like(file, perPat)) table = "Permits";
            else
            {
                _log.Warning("Archivo omitido (no coincide con patrones): {File}", file);
                return;
            }

            _log.Information("Procesando {File} -> {Schema}.{Table}", file, schema, table);

            DataTable csv = CsvUtils.ReadToDataTable(csvPath);
            var sqlSchema = loader.GetTableSchema(schema, table);
            var typed = loader.BuildTypedTable(sqlSchema);
            loader.FillTypedRows(csv, typed, sqlSchema);

            loader.Truncate(table);
            loader.BulkInsert(schema, table, typed, sqlSchema);

            _log.Information("Archivo cargado OK: {File}", file);
        }
        // probando
        private static bool Like(string text, string pattern)
        {
            if (string.IsNullOrWhiteSpace(pattern)) return false;
            var rx = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
                        .Replace("\\*", ".*")
                        .Replace("\\?", ".") + "$";
            return System.Text.RegularExpressions.Regex.IsMatch(text, rx,
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }
    }
}