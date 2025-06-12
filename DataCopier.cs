using System.Data.SqlClient;
using System.Diagnostics;

namespace CloneDataBase
{
    public static class DataCopier
    {
        public static void CopyTableData(string srcConn, string dstConn, string table, int batchSize, ref bool cancelRequested)
        {
            var stopwatch = Stopwatch.StartNew();
            int total = DatabaseUtils.GetTableCount(srcConn, table);
            int copied = 0;
            int offset = 0;

            // Obter o schema das colunas da tabela de origem
            Dictionary<string, string> columnTypes;
            using (var conn = new SqlConnection(srcConn))
            {
                conn.Open();
                using var cmd = new SqlCommand(
                    @"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table", conn);
                cmd.Parameters.AddWithValue("@table", table);
                using var reader = cmd.ExecuteReader();
                columnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                while (reader.Read())
                {
                    columnTypes[reader.GetString(0)] = reader.GetString(1);
                }
            }

            while (offset < total && !cancelRequested)
            {
                var batch = new List<Dictionary<string, object>>();
                using (var conn = new SqlConnection(srcConn))
                {
                    conn.Open();
                    var cmd = new SqlCommand($"SELECT * FROM [{table}] ORDER BY (SELECT NULL) OFFSET @offset ROWS FETCH NEXT @batch ROWS ONLY", conn);
                    cmd.Parameters.AddWithValue("@offset", offset);
                    cmd.Parameters.AddWithValue("@batch", batchSize);
                    using var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            object value = reader.GetValue(i);
                            if (value is DateTime dt)
                            {
                                // SQL Server DateTime range: 1753-01-01 to 9999-12-31
                                DateTime minSqlDate = new DateTime(1753, 1, 1);
                                DateTime maxSqlDate = new DateTime(9999, 12, 31, 23, 59, 59, 997);
                                if (dt < minSqlDate || dt > maxSqlDate)
                                {
                                    Logger.Info($"Ajustando valor de data inválido na tabela {table}, coluna {reader.GetName(i)}: {dt} -> {minSqlDate}");
                                    value = minSqlDate;
                                }
                            }
                            // Corrigir: garantir que varbinary seja byte[]
                            string colName = reader.GetName(i);
                            if (columnTypes.TryGetValue(colName, out var type) && type.StartsWith("varbinary", StringComparison.OrdinalIgnoreCase))
                            {
                                if (value == DBNull.Value)
                                {
                                    row[colName] = DBNull.Value;
                                }
                                else if (value is byte[] bytes)
                                {
                                    row[colName] = bytes;
                                }
                                else
                                {
                                    // Tenta converter para byte[]
                                    try
                                    {
                                        row[colName] = Convert.FromBase64String(value.ToString() ?? "");
                                    }
                                    catch
                                    {
                                        row[colName] = DBNull.Value;
                                    }
                                }
                            }
                            else
                            {
                                row[colName] = value;
                            }
                        }
                        batch.Add(row);
                    }
                }

                using (var connection = new SqlConnection(dstConn))
                {
                    connection.Open();

                    // Verificar se a tabela possui coluna de identidade
                    bool hasIdentity = DatabaseUtils.HasIdentityColumn(dstConn, table);

                    // 1. Ativar IDENTITY_INSERT, se aplicável
                    if (hasIdentity)
                    {
                        using (var cmd = new SqlCommand($"SET IDENTITY_INSERT [{table}] ON", connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // 2. Copiar os dados
                    foreach (var row in batch)
                    {
                        if (cancelRequested) break;
                        try
                        {
                            var cols = string.Join(",", row.Keys.Select(k => $"[{k}]"));
                            var vals = string.Join(",", row.Keys.Select(k => $"@{k}"));
                            var insertSql = $"INSERT INTO [{table}] ({cols}) VALUES ({vals})";

                            using (var insertCmd = new SqlCommand(insertSql, connection))
                            {
                                foreach (var kv in row)
                                {
                                    // Corrigir: garantir que varbinary seja byte[] no parâmetro
                                    if (columnTypes.TryGetValue(kv.Key, out var type) && type.StartsWith("varbinary", StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (kv.Value == DBNull.Value)
                                            insertCmd.Parameters.Add($"@{kv.Key}", System.Data.SqlDbType.VarBinary).Value = DBNull.Value;
                                        else
                                            insertCmd.Parameters.Add($"@{kv.Key}", System.Data.SqlDbType.VarBinary).Value = kv.Value;
                                    }
                                    else
                                    {
                                        insertCmd.Parameters.AddWithValue("@" + kv.Key, kv.Value ?? DBNull.Value);
                                    }
                                }
                                insertCmd.ExecuteNonQuery();
                            }
                            copied++;
                            Logger.Progress(table, copied, total);
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Erro ao inserir registro na tabela {table}: {ex.Message}");
                            // continue
                        }
                    }

                    // 3. Desativar IDENTITY_INSERT, se aplicável
                    if (hasIdentity)
                    {
                        using (var cmd = new SqlCommand($"SET IDENTITY_INSERT [{table}] OFF", connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                offset += batch.Count;
            }

            stopwatch.Stop();
            double totalSeconds = stopwatch.Elapsed.TotalSeconds;
            double avgPerRecordMs = copied > 0 ? stopwatch.Elapsed.TotalMilliseconds / copied : 0;

            Logger.LogCopyTiming(table, copied, totalSeconds, avgPerRecordMs);
        }
    }
}