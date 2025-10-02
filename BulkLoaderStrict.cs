using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using Serilog;

namespace Sodimac.SAP.GCP
{
    internal class BulkLoaderStrict
    {
        private readonly string _cs;
        private readonly ILogger _log;

        public BulkLoaderStrict(string connectionString, ILogger log)
        {
            _cs = connectionString;
            _log = log;
        }

        public void Truncate(string tableName)
        {
            using (var cn = new SqlConnection(_cs))
            using (var cmd = new SqlCommand("SAP.usp_TruncateTable", cn) { CommandType = CommandType.StoredProcedure })
            {
                cmd.Parameters.AddWithValue("@TableName", tableName);
                cn.Open();
                cmd.ExecuteNonQuery();
                _log.Information("TRUNCATE ejecutado para {Table}", tableName);
            }
        }

        public List<SqlColumnMeta> GetTableSchema(string schema, string table)
        {
            var cols = new List<SqlColumnMeta>();
            using (var cn = new SqlConnection(_cs))
            using (var cmd = new SqlCommand(@"
        SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, 
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=@s AND TABLE_NAME=@t
        ORDER BY ORDINAL_POSITION", cn))
            {
                cmd.Parameters.AddWithValue("@s", schema);
                cmd.Parameters.AddWithValue("@t", table);

                cn.Open();
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var name = rd.GetString(1);
                        if (name.Equals("RowId", StringComparison.OrdinalIgnoreCase) ||
                            name.Equals("LoadDate", StringComparison.OrdinalIgnoreCase))
                            continue;

                        cols.Add(new SqlColumnMeta
                        {
                            Ordinal = Convert.ToInt32(rd["ORDINAL_POSITION"]),
                            Name = name,
                            DataType = rd["DATA_TYPE"].ToString(),
                            MaxLen = rd["CHARACTER_MAXIMUM_LENGTH"] as int?,
                            Prec = rd["NUMERIC_PRECISION"] as byte?,
                            Scale = rd["NUMERIC_SCALE"] as int?
                        });
                    }
                }
            }
            return cols;
        }

        public DataTable BuildTypedTable(List<SqlColumnMeta> schema)
        {
            var dt = new DataTable();
            foreach (var c in schema)
                dt.Columns.Add(c.Name, MapClrType(c.DataType));
            return dt;
        }

        public void FillTypedRows(DataTable csv, DataTable typed, List<SqlColumnMeta> schema)
        {
            var csvCols = csv.Columns.Cast<DataColumn>().Select(c => c.ColumnName)
                         .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var missing = schema.Select(s => s.Name).Where(n => !csvCols.Contains(n)).ToList();
            if (missing.Any())
                throw new InvalidOperationException("CSV con columnas faltantes: " + string.Join(", ", missing));

            var extras = csvCols.Where(n => !schema.Any(s => s.Name.Equals(n, StringComparison.OrdinalIgnoreCase))).ToList();
            if (extras.Any())
                _log.Warning("Columnas extra en CSV (se ignorarán): {Cols}", string.Join(", ", extras));

            foreach (DataRow r in csv.Rows)
            {
                var nr = typed.NewRow();
                foreach (var col in schema)
                {
                    var raw = r[col.Name]?.ToString();
                    nr[col.Name] = string.IsNullOrWhiteSpace(raw) ? DBNull.Value : ConvertValue(raw, col);
                }
                typed.Rows.Add(nr);
            }
        }

        public void BulkInsert(string schema, string table, DataTable typed, List<SqlColumnMeta> sqlSchema)
        {
            using (var cn = new SqlConnection(_cs))
            {
                cn.Open();

                using (var bulk = new SqlBulkCopy(cn)
                {
                    DestinationTableName = $"[{schema}].[{table}]",
                    BulkCopyTimeout = 0,
                    BatchSize = 5000
                })
                {
                    foreach (var col in sqlSchema)
                        bulk.ColumnMappings.Add(col.Name, col.Name);

                    bulk.WriteToServer(typed);
                    _log.Information("Bulk insert OK: {Rows} filas en {Schema}.{Table}", typed.Rows.Count, schema, table);
                }
            }
        }

        private static Type MapClrType(string sqlType)
        {
            switch (sqlType.ToLowerInvariant())
            {
                case "int": return typeof(int);
                case "bigint": return typeof(long);
                case "smallint": return typeof(short);
                case "tinyint": return typeof(byte);
                case "bit": return typeof(bool);
                case "decimal":
                case "numeric":
                case "money":
                case "smallmoney": return typeof(decimal);
                case "float": return typeof(double);
                case "real": return typeof(float);
                case "date":
                case "datetime":
                case "datetime2":
                case "smalldatetime": return typeof(DateTime);
                case "time": return typeof(TimeSpan);
                case "uniqueidentifier": return typeof(Guid);
                default: return typeof(string);
            }
        }

        private static object ConvertValue(string raw, SqlColumnMeta meta)
        {
            try
            {
                switch (meta.DataType.ToLowerInvariant())
                {
                    case "int": return int.Parse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture);
                    case "bigint": return long.Parse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture);
                    case "smallint": return short.Parse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture);
                    case "tinyint": return byte.Parse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture);
                    case "bit":
                        if (raw == "1" || raw.Equals("true", StringComparison.OrdinalIgnoreCase)) return true;
                        if (raw == "0" || raw.Equals("false", StringComparison.OrdinalIgnoreCase)) return false;
                        if (raw.Equals("S", StringComparison.OrdinalIgnoreCase)) return true;
                        if (raw.Equals("N", StringComparison.OrdinalIgnoreCase)) return false;
                        return bool.Parse(raw);

                    case "decimal":
                    case "numeric":
                    case "money":
                    case "smallmoney":
                        var styles = NumberStyles.Number | NumberStyles.AllowCurrencySymbol;
                        if (decimal.TryParse(raw, styles, new CultureInfo("es-PE"), out var dPE)) return dPE;
                        return decimal.Parse(raw, styles, CultureInfo.InvariantCulture);

                    case "float": return double.Parse(raw, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
                    case "real": return float.Parse(raw, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);

                    case "date":
                    case "datetime":
                    case "datetime2":
                    case "smalldatetime":
                        string[] fmts = { "dd/MM/yyyy", "dd-MM-yyyy", "yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss" };
                        if (DateTime.TryParseExact(raw, fmts, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                            return dt;
                        return DateTime.Parse(raw, new CultureInfo("es-PE"));

                    case "time": return TimeSpan.Parse(raw, CultureInfo.InvariantCulture);
                    case "uniqueidentifier": return Guid.Parse(raw);

                    default:
                        if (meta.MaxLen.HasValue && meta.MaxLen.Value > 0 && raw.Length > meta.MaxLen.Value)
                            return raw.Substring(0, meta.MaxLen.Value);
                        return raw;
                }
            }
            catch (Exception ex)
            {
                throw new FormatException($"Valor inválido para columna {meta.Name} ({meta.DataType}): '{raw}'", ex);
            }
        }
    }

    internal class SqlColumnMeta
    {
        public int Ordinal { get; set; }
        public string Name { get; set; }
        public string DataType { get; set; }
        public int? MaxLen { get; set; }
        public byte? Prec { get; set; }
        public int? Scale { get; set; }
    }
}