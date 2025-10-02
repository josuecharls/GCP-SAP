using System.Configuration;
using Serilog;
using Serilog.Formatting.Json;

namespace Sodimac.SAP.GCP
{
    internal static class LoggerConfig
    {
        public static ILogger Create()
        {
            var path = ConfigurationManager.AppSettings["LogPath"];
            var fmt = (ConfigurationManager.AppSettings["LogFormat"] ?? "text").ToLower();

            var cfg = new LoggerConfiguration().MinimumLevel.Information();
            if (fmt == "json")
                cfg = cfg.WriteTo.File(new JsonFormatter(), path, rollingInterval: RollingInterval.Day);
            else
                cfg = cfg.WriteTo.File(path, rollingInterval: RollingInterval.Day);

            return cfg.CreateLogger();
        }
    }
}