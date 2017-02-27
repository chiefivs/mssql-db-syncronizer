using System;
using System.Web.Hosting;
using System.IO;

namespace DbSync
{
    internal static class Logger
    {
        private static string _logPath;
        private static string _LogPath { get { return _logPath ?? (_logPath = HostingEnvironment.MapPath("~/DbStructure/Logs/debug.log")); } }

        public static void Debug(string text)
        {
            File.AppendAllText(_LogPath, string.Format("{0:dd.MM.yyyy HH:mm:ss} {1}" + Environment.NewLine, DateTime.Now, text));
        }

        public static void Error(){}
    }
}
