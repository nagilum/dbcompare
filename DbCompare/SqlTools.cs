using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using Dapper;

namespace DbCompare {
    public class SqlTools {
        /// <summary>
        /// Create and open an SQL connection.
        /// </summary>
        public static SqlConnection GetSqlConnection(
            string hostname,
            string username,
            string password,
            string database) {

            try {
                var connection = new SqlConnection(string.Format(
                    "Data Source={0}; User ID={1}; Password={2}; Initial Catalog={3};",
                    hostname,
                    username,
                    password,
                    database));

                connection.Open();

                Console.WriteLine();
                Console.WriteLine(
                    "Connected to {0}@{1}/{2}",
                    username,
                    hostname,
                    database);

                return connection;
            }
            catch (Exception ex) {
                Console.WriteLine("ERROR!");
                Console.WriteLine("Error creating and/or connecting to source database.");
                Console.WriteLine(ex.Message);

                if (ex.InnerException != null) {
                    Console.WriteLine(ex.InnerException.Message);
                }

                return null;
            }
        }

        /// <summary>
        /// Compare tables to see if any are missing.
        /// </summary>
        public static void CompareFullTables(
            IDbConnection sdbc,
            IDbConnection tdbc,
            string sd,
            string td,
            bool createSqlFiles,
            out List<string> stables,
            out List<string> ttables) {

            // Get source tables.
            stables = sdbc.Query<string>(string.Format(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = '{0}' ORDER BY TABLE_NAME ASC;",
                sd)).ToList();

            // Get target tables.
            ttables = tdbc.Query<string>(string.Format(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = '{0}' ORDER BY TABLE_NAME ASC;",
                td)).ToList();

            // Check for missing target tables.
            Console.WriteLine();
            Console.WriteLine("Checking for missing target tables..");

            var missing = 0;

            foreach (var table in stables) {
                if (ttables.Contains(table)) {
                    continue;
                }

                missing++;

                Console.WriteLine(
                    "- Missing table: {0}",
                    table);

                if (!createSqlFiles) {
                    continue;
                }

                // Compile the CREATE TABLE script.
                CompileCreateTableScript(
                    sdbc,
                    table);
            }

            if (missing > 0) {
                return;
            }

            Console.WriteLine("..No missing tables found.");
        }

        /// <summary>
        /// Compare columns for each table.
        /// </summary>
        public static void CompareTableColumns(
            IDbConnection sdbc,
            IDbConnection tdbc,
            List<string> stables,
            List<string> ttables,
            bool createSqlFiles) {

            // Check for missing target table columns.
            Console.WriteLine();
            Console.WriteLine("Checking for missing target table columns..");

            foreach (var table in stables) {
                if (!ttables.Contains(table)) {
                    continue;
                }

                var scolumns = sdbc.Query<TableColumn>(string.Format(
                    "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' ORDER BY ORDINAL_POSITION ASC;",
                    table)).ToList();

                var tcolumns = tdbc.Query<TableColumn>(string.Format(
                    "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' ORDER BY ORDINAL_POSITION ASC;",
                    table)).ToList();

                var missingColumns = new List<TableColumn>();

                foreach (var column in scolumns) {
                    if (tcolumns.Any(n => n.COLUMN_NAME == column.COLUMN_NAME)) {
                        continue;
                    }

                    Console.WriteLine(
                        "- Missing column {0} in table {1}",
                        column.COLUMN_NAME,
                        table);

                    if (!createSqlFiles) {
                        continue;
                    }

                    // Add column to list.
                    missingColumns.Add(column);
                }

                if (!createSqlFiles ||
                    missingColumns.Count == 0) {

                    continue;
                }

                // Compile update script and write file.
                CompileAlterTableScript(
                    table,
                    missingColumns);
            }
        }

        /// <summary>
        /// Compile the CREATE TABLE script.
        /// </summary>
        public static void CompileCreateTableScript(
            IDbConnection dbc,
            string tableName) {

            var sql = new StringBuilder();

            sql.AppendLine(string.Format(
                "CREATE TABLE [{0}] (", tableName));

            var columns = dbc.Query<TableColumn>(string.Format(
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' ORDER BY ORDINAL_POSITION ASC;",
                tableName)).ToList();

            var constraints = dbc.Query<Constrant>(string.Format(
                "SELECT " +
                "ccu.COLUMN_NAME, tc.CONSTRAINT_TYPE " +
                "FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu " +
                "LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                "ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND ccu.TABLE_NAME = tc.TABLE_NAME " +
                "WHERE ccu.TABLE_NAME = '{0}';",
                tableName)).ToList();

            var count = 0;
            var max = columns.Count;
            var addPk = false;
            var pkColumnName = string.Empty;

            // Add columns.
            foreach (var column in columns) {
                count++;

                // Name
                sql.Append(string.Format(
                    "\t[{0}]",
                    column.COLUMN_NAME));

                // DataType
                sql.Append(string.Format(
                    " [{0}]",
                    column.DATA_TYPE));

                // Contraints
                if (column.CHARACTER_MAXIMUM_LENGTH.HasValue) {
                    sql.Append(string.Format(
                        "({0})",
                        column.CHARACTER_MAXIMUM_LENGTH));
                }

                // Decimal?
                if (column.DATA_TYPE == "decimal" &&
                    column.NUMERIC_PRECISION.HasValue &&
                    column.NUMERIC_SCALE.HasValue) {

                    sql.Append(string.Format(
                        "({0},{1})",
                        column.NUMERIC_PRECISION.Value,
                        column.NUMERIC_SCALE.Value));
                }

                // Identity
                if (constraints.Any(n => n.COLUMN_NAME == column.COLUMN_NAME &&
                                         n.CONSTRAINT_TYPE == "PRIMARY KEY") &&
                    column.DATA_TYPE == "int") {

                    sql.Append(" IDENTITY(1,1)");
                    addPk = true;
                    pkColumnName = column.COLUMN_NAME;
                }

                // Nullable
                sql.Append(string.Format(
                    " {0} NULL",
                    column.IS_NULLABLE == "YES"
                        ? ""
                        : "NOT"));

                // Add comma?
                sql.AppendLine(count < max || addPk ? "," : "");
            }

            if (addPk) {
                sql.AppendLine(string.Format(
                    "\tCONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ([{1}] ASC)",
                    tableName,
                    pkColumnName));
            }

            sql.AppendLine(");");

            // Save file.
            var filename = string.Format(
                "CREATE_TABLE_{0}.sql",
                tableName);

            var path = Path.Combine(
                Directory.GetCurrentDirectory(),
                filename);

            Console.WriteLine(
                "  .. Writing SQL script to {0}",
                filename);

            File.WriteAllText(
                path,
                sql.ToString());
        }

        /// <summary>
        /// Compile the ALTER TABLE script.
        /// </summary>
        public static void CompileAlterTableScript(
            string tableName,
            List<TableColumn> missingColumns) {

            var sql = new StringBuilder();
            var count = 0;
            var max = missingColumns.Count;

            sql.AppendLine(string.Format(
                "ALTER TABLE [{0}] ADD",
                tableName));

            foreach (var column in missingColumns) {
                count++;

                // Name
                sql.Append(string.Format(
                    "\t[{0}]",
                    column.COLUMN_NAME));

                // DataType
                sql.Append(string.Format(
                    " [{0}]",
                    column.DATA_TYPE));

                // Contraints
                if (column.CHARACTER_MAXIMUM_LENGTH.HasValue) {
                    sql.Append(string.Format(
                        "({0})",
                        column.CHARACTER_MAXIMUM_LENGTH));
                }

                // Decimal?
                if (column.DATA_TYPE == "decimal" &&
                    column.NUMERIC_PRECISION.HasValue &&
                    column.NUMERIC_SCALE.HasValue) {

                    sql.Append(string.Format(
                        "({0},{1})",
                        column.NUMERIC_PRECISION.Value,
                        column.NUMERIC_SCALE.Value));
                }

                // Nullable
                sql.Append(string.Format(
                    " {0} NULL",
                    column.IS_NULLABLE == "YES"
                        ? ""
                        : "NOT"));

                // Add comma?
                sql.AppendLine(count < max ? "," : "");
            }

            sql.AppendLine(";");

            // Save file.
            var filename = string.Format(
                "ALTER_TABLE_{0}_ADD_{1}_COLUMN{2}.sql",
                tableName,
                missingColumns.Count,
                missingColumns.Count == 1 ? "" : "S");

            var path = Path.Combine(
                Directory.GetCurrentDirectory(),
                filename);

            Console.WriteLine(
                "  .. Writing SQL script to {0}",
                filename);

            File.WriteAllText(
                path,
                sql.ToString());
        }

        public class TableColumn {
            public string TABLE_CATALOG { get; set; }
            public string TABLE_SCHEMA { get; set; }
            public string TABLE_NAME { get; set; }
            public string COLUMN_NAME { get; set; }
            public int ORDINAL_POSITION { get; set; }
            public string COLUMN_DEFAULT { get; set; }
            public string IS_NULLABLE { get; set; }
            public string DATA_TYPE { get; set; }
            public int? CHARACTER_MAXIMUM_LENGTH { get; set; }
            public int? CHARACTER_OCTET_LENGTH { get; set; }
            public int? NUMERIC_PRECISION { get; set; }
            public int? NUMERIC_PRECISION_RADIX { get; set; }
            public int? NUMERIC_SCALE { get; set; }
            public int? DATETIME_PRECISION { get; set; }
            public string CHARACTER_SET_CATALOG { get; set; }
            public string CHARACTER_SET_SCHEMA { get; set; }
            public string CHARACTER_SET_NAME { get; set; }
            public string COLLATION_CATALOG { get; set; }
            public string COLLATION_SCHEMA { get; set; }
            public string COLLATION_NAME { get; set; }
            public string DOMAIN_CATALOG { get; set; }
            public string DOMAIN_SCHEMA { get; set; }
            public string DOMAIN_NAME { get; set; }
        }

        public class Constrant {
            public string COLUMN_NAME { get; set; }
            public string CONSTRAINT_TYPE { get; set; }
        }
    }
}