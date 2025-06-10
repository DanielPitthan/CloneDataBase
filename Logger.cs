using System.IO;
using System.Text;
using System.Globalization;

namespace CloneDataBase
{
    public static class Logger
    {
        private static StreamWriter? _log;
        private static object _lock = new();
        private static string _logDir = @"C:\CloneDataBaseWorkDir";
        private static string _logFileName = "CloneDataBase.log.txt";
        private static string _logFilePath = "";
        private const long MaxLogSize = 5 * 1024 * 1024; // 5MB

        public static void Init()
        {
            try
            {
                if (!Directory.Exists(_logDir))
                    Directory.CreateDirectory(_logDir);

                _logFilePath = Path.Combine(_logDir, _logFileName);
                _log = new StreamWriter(_logFilePath, true, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERRO: Falha ao inicializar o log: {ex.Message}");
                Console.ForegroundColor = ConsoleColor.White;
            }
        }

        public static void StartSession()
        {
            WriteLog($"Início da execução: {DateTime.Now}");
        }

        public static void EndSession(DateTime start)
        {
            var elapsed = DateTime.Now - start;
            WriteLog($"Fim da execução: {DateTime.Now} (Duração: {elapsed})");
            try
            {
                _log?.Flush();
                _log?.Close();
                _log = null; // Prevent further writes to a closed StreamWriter
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERRO: Falha ao finalizar o log: {ex.Message}");
                Console.ForegroundColor = ConsoleColor.White;
            }
        }

        public static void Info(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.Write("INFO: ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine(msg);
            WriteLog("INFO: " + msg);
        }

        public static void Error(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Write("ERRO: ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine(msg);
            WriteLog("ERRO: " + msg);
        }

        public static void ObjectCopied(string obj, string status)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write($"{obj} ");
            Console.Write("OK");
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine();
            WriteLog($"OBJETO: {obj} OK");
        }

        public static void Progress(string table, int current, int total)
        {
            var percent = (int)((current / (double)total) * 100);
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write($"\rTabela {table}: {current}/{total} ({percent}%)   ");
            if (current == total) Console.WriteLine();
            WriteLog($"PROGRESSO: Tabela {table}: {current}/{total} ({percent}%)");
        }

        public static void LogCopyTiming(string table, int totalCopied, double totalSeconds, double avgPerRecordMs)
        {
            string msg = $"Tabela '{table}': {totalCopied} registros copiados em {totalSeconds.ToString("F3", CultureInfo.InvariantCulture)} segundos. " +
                         $"Tempo médio por registro: {avgPerRecordMs.ToString("F3", CultureInfo.InvariantCulture)} ms.";
            Info(msg);
        }

        public static void LogDateAdjustment(string columnName, DateTime originalValue, DateTime adjustedValue)
        {
            string msg = $"Ajuste de data realizado na coluna '{columnName}': Valor original '{originalValue:yyyy-MM-dd HH:mm:ss}', Valor ajustado '{adjustedValue:yyyy-MM-dd HH:mm:ss}'.";
            Info(msg);
        }

        private static void WriteLog(string msg)
        {
            lock (_lock)
            {
                try
                {
                    // Prevent writing if log is closed or not initialized
                    if (_log == null)
                        return;

                    RotateLogIfNeeded();
                    _log?.WriteLine($"{DateTime.Now:HH:mm:ss} {msg}");
                    _log?.Flush();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"ERRO: Falha ao gravar no log: {ex.Message}");
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }

        private static void RotateLogIfNeeded()
        {
            try
            {
                if (string.IsNullOrEmpty(_logFilePath) || !File.Exists(_logFilePath))
                    return;

                var fileInfo = new FileInfo(_logFilePath);
                if (fileInfo.Length >= MaxLogSize)
                {
                    _log?.Flush();
                    _log?.Close();

                    string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                    string rotatedName = $"clone_log_{timestamp}.txt";
                    string rotatedPath = Path.Combine(_logDir, rotatedName);

                    File.Move(_logFilePath, rotatedPath, true);

                    _log = new StreamWriter(_logFilePath, false, Encoding.UTF8);
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERRO: Falha ao rotacionar o log: {ex.Message}");
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }
}