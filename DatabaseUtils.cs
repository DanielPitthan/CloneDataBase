using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic; // Adicionado para List e Dictionary

namespace CloneDataBase
{
    public static class DatabaseUtils
    {
        public static bool TestConnection(string connStr)
        {
            try
            {
                using var conn = new SqlConnection(connStr);
                conn.Open();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static List<string> GetTableNames(string connStr)
        {
            var tables = new List<string>();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                tables.Add(reader.GetString(0));
            return tables;
        }

        // Novo método: retorna apenas tabelas de usuário (exclui tabelas de sistema)
        public static List<string> GetUserTableNames(string connStr)
        {
            var tables = new List<string>();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            // Somente USER_TABLES (exclui system tables)
            string sql = @"
                SELECT name 
                FROM sys.tables 
                WHERE is_ms_shipped = 0
                ORDER BY name";
            using var cmd = new SqlCommand(sql, conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                tables.Add(reader.GetString(0));
            return tables;
        }

      
        public static Dictionary<string, string>? GetTableSchema(string connectionString, string tableName)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();

            // Verifica se a tabela existe
            var checkCmd = new SqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @table", conn);
            checkCmd.Parameters.AddWithValue("@table", tableName);
            var existsObj = checkCmd.ExecuteScalar();
            int exists = existsObj is int i ? i : Convert.ToInt32(existsObj);
            if (exists == 0)
                return null;

            // Obtém o schema da tabela
            var schemaCmd = new SqlCommand(
                @"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_NAME = @table
                  ORDER BY ORDINAL_POSITION", conn);
            schemaCmd.Parameters.AddWithValue("@table", tableName);

            var schema = new Dictionary<string, string>();
            using var reader = schemaCmd.ExecuteReader();
            while (reader.Read())
            {
                string col = reader.GetString(0);
                string type = reader.GetString(1);
                object maxLenObj = reader["CHARACTER_MAXIMUM_LENGTH"];
                if (maxLenObj != DBNull.Value && (type == "nvarchar" || type == "varchar" || type == "char"))
                    type += $"({((maxLenObj is int && (int)maxLenObj == -1) ? "MAX" : maxLenObj)})";
                schema[col] = type;
            }
            return schema;
        }

        // Novo método para obter o schema da tabela como DataTable
        public static DataTable GetTableSchemaDataTable(string connectionString, string tableName)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            using var cmd = new SqlCommand($"SELECT TOP 0 * FROM [{tableName}]", conn);
            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
            return reader.GetSchemaTable()!;
        }

        public static void CreateTable(string connectionString, Dictionary<string, string> schema, string tableName)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();

            var columns = schema.Select(kvp => $"[{kvp.Key}] {kvp.Value}");
            var createSql = $"CREATE TABLE [{tableName}] ({string.Join(", ", columns)})";
            using var cmd = new SqlCommand(createSql, conn);
            cmd.ExecuteNonQuery();
        }

        public static void AlterTable(string connStr, DataTable srcSchema, DataTable dstSchema)
        {
            var alterSqls = SchemaComparer.GenerateAlterTableSql(srcSchema, dstSchema);
            using var conn = new SqlConnection(connStr);
            conn.Open();
            foreach (var sql in alterSqls)
            {
                using var cmd = new SqlCommand(sql, conn);
                cmd.ExecuteNonQuery();
            }
        }

        public static int GetTableCount(string connStr, string table)
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{table}]", conn);
            var result = cmd.ExecuteScalar();
            return result is int i ? i : Convert.ToInt32(result);
        }

        // Adiciona método para verificar se a tabela possui coluna IDENTITY
        public static bool HasIdentityColumn(string connectionString, string tableName)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            var sql = @"
                SELECT COUNT(*) 
                FROM sys.columns c
                INNER JOIN sys.tables t ON c.object_id = t.object_id
                WHERE t.name = @table AND c.is_identity = 1";
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@table", tableName);
            var result = cmd.ExecuteScalar();
            return (result is int i ? i : Convert.ToInt32(result)) > 0;
        }

        public static bool DatabaseExists(string connectionString, string databaseName)
        {
            try
            {
                using var conn = new SqlConnection(connectionString);
                conn.Open();
                using var cmd = new SqlCommand(
                    "SELECT COUNT(*) FROM sys.databases WHERE name = @dbName", conn);
                cmd.Parameters.AddWithValue("@dbName", databaseName);
                int count = (int)cmd.ExecuteScalar();
                return count > 0;
            }
            catch (Exception ex)
            {
                Logger.Error($"Erro ao verificar existência do banco de dados '{databaseName}': {ex.Message}");
                return false;
            }
        }

        public static bool DropDatabase(string connectionString, string databaseName)
        {
            try
            {
                // Altera a string de conexão para usar o banco 'master'
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "master"
                };
                using var conn = new SqlConnection(builder.ConnectionString);
                conn.Open();
                // Set single user mode and drop
                string sql = $@"
                    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @dbName)
                    BEGIN
                        ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        DROP DATABASE [{databaseName}];
                    END";
                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@dbName", databaseName);
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Erro ao apagar banco de dados '{databaseName}': {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Cria o banco de dados no destino, conectando-se ao banco 'master'.
        /// </summary>
        public static bool CreateDatabase(string connectionString, string databaseName)
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "master"
                };
                using var conn = new SqlConnection(builder.ConnectionString);
                conn.Open();
                string sql = $"CREATE DATABASE [{databaseName}]";
                using var cmd = new SqlCommand(sql, conn);
                cmd.ExecuteNonQuery();
                Logger.Info($"Banco de dados '{databaseName}' criado com sucesso no destino.");
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Erro ao criar banco de dados '{databaseName}': {ex.Message}");
                return false;
            }
        }

        internal static string GetDatabaseName(string destConn)
        {
            // Verifica se a string de conexão está vazia ou nula
            if (string.IsNullOrEmpty(destConn))
            {
                throw new ArgumentException("A string de conexão não pode ser nula ou vazia.", nameof(destConn));
            }

            // Divide a string de conexão em partes usando ';' como delimitador
            var parts = destConn.Split(';');

            // Procura pela parte que contém o nome do banco de dados
            foreach (var part in parts)
            {
                if (part.Trim().StartsWith("Database=", StringComparison.OrdinalIgnoreCase))
                {
                    // Retorna o nome do banco de dados, removendo o prefixo "Database="
                    return part.Substring("Database=".Length).Trim();
                }
            }

            // Se não encontrar, lança uma exceção
            throw new InvalidOperationException("Nome do banco de dados não encontrado na string de conexão.");
        }
    }
}