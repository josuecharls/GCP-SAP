using System.Data;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace Sodimac.SAP.GCP
{
    internal static class CsvUtils
    {
        public static DataTable ReadToDataTable(string csvPath)
        {
            var dt = new DataTable();
            var cfg = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                DetectDelimiter = true,
                BadDataFound = null,
                MissingFieldFound = null,
                IgnoreBlankLines = true
            };

            using (var reader = new StreamReader(csvPath))
            using (var csv = new CsvReader(reader, cfg))
            {
                csv.Read();
                csv.ReadHeader();
                foreach (var h in csv.HeaderRecord)
                    dt.Columns.Add(h, typeof(string));

                while (csv.Read())
                {
                    var row = dt.NewRow();
                    foreach (DataColumn c in dt.Columns)
                        row[c.ColumnName] = csv.GetField(c.ColumnName);
                    dt.Rows.Add(row);
                }
            }

            return dt;
        }
    }
}