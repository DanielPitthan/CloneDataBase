using System.Data.SqlClient;
using System.Text;
using CloneDataBase;

namespace CloneDataBase
{
    internal class Program
    {
        static bool _cancelRequested = false;

        static void ShowAcceptanceTermAndWait()
        {
            Console.Clear();
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("###############################################################");
            Console.WriteLine("Eu concordo que estou fazendo essa operação por conta e risco,");
            Console.WriteLine("que ela não possui garantia de sucesso e que os dados no destino");
            Console.WriteLine("poderão ser corrompidos.");
            Console.WriteLine("Afirmo que realizei todos os procedimentos de backup e cópias de segurança.");
            Console.WriteLine("O processo poderá ser interrompido a qualquer momento,");
            Console.WriteLine("porém isso poderá causar perda de dados.");
            Console.WriteLine("###############################################################");
            Console.ResetColor();
            Console.WriteLine();
            Console.WriteLine("Para aceitar, pressione a letra X. [ ]");

            // Espera pelo X (maiúsculo ou minúsculo)
            int left = Math.Max(Console.CursorLeft - 2, 0);
            int top = Math.Max(Console.CursorTop - 1, 0);
            // Garante que left não ultrapasse o buffer width
            if (left >= Console.BufferWidth) left = Console.BufferWidth - 1;
            if (top >= Console.BufferHeight) top = Console.BufferHeight - 1;
            while (true)
            {
                var key = Console.ReadKey(true);
                if (key.KeyChar == 'x' || key.KeyChar == 'X')
                {
                    Console.SetCursorPosition(left, top);
                    Console.Write("X");
                    break;
                }
            }
            Console.WriteLine();
            Console.WriteLine();
        }

        static void ShowFooter()
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine();
            Console.WriteLine("Ao final do processo um arquivo de log será gerado na pasta: C:\\CloneDataBaseWorkDir com o detalhamento do processo.");
            Console.ResetColor();
        }

        static void Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.BackgroundColor = ConsoleColor.Black;
            Console.Clear();

            Logger.Init();

            Console.CancelKeyPress += (s, e) =>
            {
                _cancelRequested = true;
                Logger.Info("Interrupção manual solicitada. Finalizando após o batch atual...");
                e.Cancel = true;
            };

            string sourceConn, destConn;
            bool dropDestinationDb = false;

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.Write("Deseja apagar o banco de dados no destino antes de iniciar a cópia? (s/n): ");
            Console.ForegroundColor = ConsoleColor.White;
            var dropInput = Console.ReadLine();
            if (!string.IsNullOrEmpty(dropInput) && dropInput.Trim().ToLower() == "s")
            {
                dropDestinationDb = true;
            }

            while (true)
            {
                Logger.Info("Informe a connection string de ORIGEM:");
                sourceConn = Console.ReadLine() ?? "";
                Logger.Info("Informe a connection string de DESTINO:");
                destConn = Console.ReadLine() ?? "";

                // Verificação se as connection strings são iguais
                if (sourceConn.Trim() == destConn.Trim())
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.BackgroundColor = ConsoleColor.Black;
                    Console.WriteLine();
                    Console.WriteLine("###############################################################");
                    Console.WriteLine("##                  Connection Strings Iguais                ##");
                    Console.WriteLine("##              Não é possível continuar                     ##");
                    Console.WriteLine("###############################################################");
                    Console.ResetColor();
                    ShowFooter();
                    Environment.Exit(1);
                }

                if (!DatabaseUtils.TestConnection(sourceConn))
                {
                    Logger.Error("Não foi possível conectar à base de ORIGEM.");
                    continue;
                }
                if (!DatabaseUtils.TestConnection(destConn))
                {
                    Logger.Error("Não foi possível conectar à base de DESTINO.");
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.Write("Deseja criar o banco de dados no destino? (s/n): ");
                    Console.ForegroundColor = ConsoleColor.White;
                    var resp = Console.ReadLine();
                    if (!string.IsNullOrEmpty(resp) && resp.Trim().ToLower() == "s")
                    {
                        try
                        {
                            string _destDatabaseName = DatabaseUtils.GetDatabaseName(destConn);
                            if (DatabaseUtils.CreateDatabase(destConn, _destDatabaseName))
                            {
                                Logger.Info($"Banco de dados '{_destDatabaseName}' criado com sucesso no destino.");
                                // Aguarda um pouco para garantir que o banco foi criado
                                System.Threading.Thread.Sleep(2000);
                                // Tenta novamente a conexão
                                if (!DatabaseUtils.TestConnection(destConn))
                                {
                                    Logger.Error("Ainda não foi possível conectar ao banco de dados de DESTINO após a criação. Processo abortado.");
                                    Environment.Exit(1);
                                }
                            }
                            else
                            {
                                Logger.Error("Falha ao criar o banco de dados no destino. Processo abortado.");
                                Environment.Exit(1);
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Erro ao criar banco de dados no destino: {ex.Message}");
                            Console.WriteLine("Processo abortado");
                            Environment.Exit(1);
                        }
                    }
                    else
                    {
                        Console.WriteLine("Processo abortado");
                        Environment.Exit(1);
                    }
                }
                break;
            }

            string destDatabaseName = DatabaseUtils.GetDatabaseName(destConn);
            if (!DatabaseUtils.DatabaseExists(destConn, destDatabaseName))
            {
                Logger.Info($"Banco de dados '{destDatabaseName}' não existe no destino. Criando banco de dados...");
                if (!DatabaseUtils.CreateDatabase(destConn, destDatabaseName))
                {
                    Logger.Error($"Falha ao criar o banco de dados '{destDatabaseName}' no destino. Abortando operação.");
                    Environment.Exit(1);
                }
                // Opcional: aguardar alguns segundos para garantir que o banco foi criado antes de prosseguir
                System.Threading.Thread.Sleep(2000);
            }
            else if (DatabaseUtils.DatabaseExists(destConn, destDatabaseName))
            {
                if (dropDestinationDb)
                {
                    Logger.Info($"Banco de dados '{destDatabaseName}' existe no destino e será apagado.");
                    if (DatabaseUtils.DropDatabase(destConn, destDatabaseName))
                    {
                        Logger.Info($"Banco de dados '{destDatabaseName}' apagado com sucesso no destino.");
                    }
                    else
                    {
                        Logger.Error($"Falha ao apagar o banco de dados '{destDatabaseName}' no destino.");
                        Environment.Exit(1);
                    }
                }
                else
                {
                    Logger.Info($"Aviso: O banco de dados '{destDatabaseName}' já existe no destino. Estruturas existentes podem causar erros durante a cópia.");
                }
            }
            else
            {
                Logger.Info($"Banco de dados '{destDatabaseName}' não existe no destino. Prosseguindo com a cópia.");
            }

            // Exibe termo de aceite e aguarda confirmação
            ShowAcceptanceTermAndWait();

            // Exibe rodapé fixo
            ShowFooter();

            Logger.Info("Escolha o modo de cópia:");
            Logger.Info("1 - Cópia completa");
            Logger.Info("2 - Cópia parcial (informe as tabelas separadas por vírgula)");
            string? option = Console.ReadLine();
            List<string> tablesToCopy;
            if (option == "2")
            {
                Logger.Info("Digite os nomes das tabelas (separados por vírgula):");
                var input = Console.ReadLine() ?? "";
                tablesToCopy = input.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
            }
            else
            {
                tablesToCopy = DatabaseUtils.GetUserTableNames(sourceConn);
            }

            Logger.Info("Informe o batch size (padrão 1000):");
            int batchSize = 1000;
            var batchInput = Console.ReadLine();
            if (int.TryParse(batchInput, out int userBatch) && userBatch > 0)
                batchSize = userBatch;

            var startTime = DateTime.Now;
            Logger.StartSession();

            foreach (var table in tablesToCopy)
            {
                if (_cancelRequested) break;

                Logger.Info($"Processando tabela: {table}");
                try
                {
                    var schemaSrc = DatabaseUtils.GetTableSchema(sourceConn, table);
                    var schemaDst = DatabaseUtils.GetTableSchema(destConn, table);

                    if (schemaSrc == null)
                    {
                        Logger.Error($"Tabela {table} não encontrada na origem. Pulando...");
                        continue;
                    }

                    if (schemaDst == null)
                    {
                        Logger.Info($"Tabela {table} não existe no destino. Criando...");
                        DatabaseUtils.CreateTable(destConn, schemaSrc, table);
                        Logger.ObjectCopied(table, "OK");
                    }
                    else
                    {
                        if (!SchemaComparer.AreSchemasEqual(schemaSrc, schemaDst))
                        {
                            Logger.Info($"Ajustando estrutura da tabela {table} no destino...");
                            var srcSchemaTable = DatabaseUtils.GetTableSchemaDataTable(sourceConn, table);
                            var dstSchemaTable = DatabaseUtils.GetTableSchemaDataTable(destConn, table);
                            DatabaseUtils.AlterTable(destConn, srcSchemaTable, dstSchemaTable);
                        }
                    }

                    DataCopier.CopyTableData(sourceConn, destConn, table, batchSize, ref _cancelRequested);
                }
                catch (Exception ex)
                {
                    Logger.Error($"Erro ao copiar tabela {table}: {ex.Message}");
                }
            }

            // Pós-cópia: verificação opcional
            Logger.Info("Deseja realizar verificação pós-cópia? (s/n)");
            var verify = Console.ReadLine();
            if (verify?.ToLower() == "s")
            {
                foreach (var table in tablesToCopy)
                {
                    if (_cancelRequested) break;
                    try
                    {
                        var srcCount = DatabaseUtils.GetTableCount(sourceConn, table);
                        var dstCount = DatabaseUtils.GetTableCount(destConn, table);
                        if (srcCount == dstCount)
                            Logger.Info($"Tabela {table}: contagem OK ({srcCount})");
                        else
                            Logger.Error($"Tabela {table}: contagem divergente (origem: {srcCount}, destino: {dstCount})");
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Erro na verificação da tabela {table}: {ex.Message}");
                    }
                }
            }

            Logger.EndSession(startTime);
            Logger.Info("Processo finalizado. Pressione qualquer tecla para sair.");
            Console.ReadKey();
        }
    }
}
