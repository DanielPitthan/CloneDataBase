using System.Data;

namespace CloneDataBase
{
    public static class SchemaComparer
    {
        public static bool AreSchemasEqual(DataTable src, DataTable dst)
        {
            // Simplified: compare columns count and names/types
            if (src.Rows.Count != dst.Rows.Count) return false;
            for (int i = 0; i < src.Rows.Count; i++)
            {
                if (!src.Rows[i]["ColumnName"].Equals(dst.Rows[i]["ColumnName"])) return false;
                if (!src.Rows[i]["DataType"].Equals(dst.Rows[i]["DataType"])) return false;
            }
            return true;
        }

        public static string GenerateCreateTableSql(DataTable schema)
        {
            // Simplified: only columns and types
            var tableName = schema.Rows[0]["BaseTableName"].ToString();
            var cols = new List<string>();
            foreach (DataRow row in schema.Rows)
            {
                var col = $"[{row["ColumnName"]}] {GetSqlType(row)}";
                if ((bool)row["IsIdentity"])
                    col += " IDENTITY(1,1)";
                if (!(bool)row["AllowDBNull"])
                    col += " NOT NULL";
                cols.Add(col);
            }
            return $"CREATE TABLE [{tableName}] ({string.Join(", ", cols)})";
        }

        public static List<string> GenerateAlterTableSql(DataTable src, DataTable dst)
        {
            // Simplified: add missing columns
            var srcCols = src.Rows.Cast<DataRow>().Select(r => r["ColumnName"].ToString()).ToHashSet();
            var dstCols = dst.Rows.Cast<DataRow>().Select(r => r["ColumnName"].ToString()).ToHashSet();
            var missing = srcCols.Except(dstCols);
            var tableName = src.Rows[0]["BaseTableName"].ToString();
            var sqls = new List<string>();
            foreach (var col in missing)
            {
                var row = src.Rows.Cast<DataRow>().First(r => r["ColumnName"].ToString() == col);
                var colDef = $"[{col}] {GetSqlType(row)}";
                if ((bool)row["IsIdentity"])
                    colDef += " IDENTITY(1,1)";
                if (!(bool)row["AllowDBNull"])
                    colDef += " NOT NULL";
                sqls.Add($"ALTER TABLE [{tableName}] ADD {colDef}");
            }
            return sqls;
        }

        private static string GetSqlType(DataRow row)
        {
            // Map .NET type to SQL type (simplified)
            var dataType = row["DataType"] as Type;
            if (dataType == typeof(int)) return "INT";
            if (dataType == typeof(long)) return "BIGINT";
            if (dataType == typeof(string)) return "NVARCHAR(MAX)";
            if (dataType == typeof(DateTime)) return "DATETIME";
            if (dataType == typeof(bool)) return "BIT";
            if (dataType == typeof(decimal)) return "DECIMAL(18,2)";
            // Add more as needed
            return "NVARCHAR(MAX)";
        }

        internal static bool AreSchemasEqual(Dictionary<string, string> schemaSrc, Dictionary<string, string> schemaDst)
        {
            // Verifica se o número de chaves é diferente
            if (schemaSrc.Count != schemaDst.Count)
            {
                return false;
            }

            // Compara cada chave e valor
            foreach (var kvp in schemaSrc)
            {
                if (!schemaDst.TryGetValue(kvp.Key, out var valueDst) || valueDst != kvp.Value)
                {
                    return false;
                }
            }

            return true;
        }
    }
}