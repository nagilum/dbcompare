using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DbCompare {
    public class Program {
        private static void Main(string[] args) {
            Console.WriteLine("DbCompare v1.0");

            if (args.Length == 0) {
                ShowHelpScreen();
                return;
            }

            var sh = GetArgsValue(args, "sh");
            var su = GetArgsValue(args, "su");
            var sp = GetArgsValue(args, "sp");
            var sd = GetArgsValue(args, "sd");

            var th = GetArgsValue(args, "th");
            var tu = GetArgsValue(args, "tu");
            var tp = GetArgsValue(args, "tp");
            var td = GetArgsValue(args, "td");

            var cf = GetArgsSwitchBool(args, "cf");

            if (string.IsNullOrWhiteSpace(sh) ||
                string.IsNullOrWhiteSpace(su) ||
                string.IsNullOrWhiteSpace(sp) ||
                string.IsNullOrWhiteSpace(sd) ||

                string.IsNullOrWhiteSpace(th) ||
                string.IsNullOrWhiteSpace(tu) ||
                string.IsNullOrWhiteSpace(tp) ||
                string.IsNullOrWhiteSpace(td)) {

                ShowHelpScreen();
                return;
            }

            if (cf) {
                Console.WriteLine();
                Console.WriteLine(
                    "Writing SQL files to {0}",
                    Directory.GetCurrentDirectory());
            }

            // Create and open SQL connections.
            var sdbc = SqlTools.GetSqlConnection(sh, su, sp, sd);
            var tdbc = SqlTools.GetSqlConnection(th, tu, tp, td);

            if (sdbc == null ||
                tdbc == null) {

                return;
            }

            // Compare tables to see if any are missing.
            SqlTools.CompareFullTables(
                sdbc,
                tdbc,
                sd,
                td,
                cf,
                out var stables,
                out var ttables);

            // Compare columns for each table.
            SqlTools.CompareTableColumns(
                sdbc,
                tdbc,
                stables,
                ttables,
                cf);

            // Done, close connections.
            sdbc.Close();
            tdbc.Close();
        }

        /// <summary>
        /// Get value from the args array as parameter.
        /// </summary>
        private static string GetArgsValue(
            IReadOnlyList<string> args,
            string key) {

            var max = args.Count - 1;

            for (var i = 0; i < max; i++) {
                if (args[i] == "-" + key) {
                    return args[i + 1];
                }
            }

            return null;
        }

        /// <summary>
        /// Get boolean switch from args.
        /// </summary>
        private static bool GetArgsSwitchBool(
            IEnumerable<string> args,
            string key) {
            
            return args.Any(arg => arg == "-" + key);
        }

        /// <summary>
        /// Display help info.
        /// </summary>
        private static void ShowHelpScreen() {
            Console.WriteLine("Will compare source with target and report missing");
            Console.WriteLine("tables and colums from the target database.");
            Console.WriteLine();
            Console.WriteLine(" Source:");
            Console.WriteLine(" -sh <hostname>");
            Console.WriteLine(" -su <username>");
            Console.WriteLine(" -sp <password>");
            Console.WriteLine(" -sd <database>");
            Console.WriteLine();
            Console.WriteLine(" Target:");
            Console.WriteLine(" -th <hostname>");
            Console.WriteLine(" -tu <username>");
            Console.WriteLine(" -tp <password>");
            Console.WriteLine(" -td <database>");
            Console.WriteLine();
            Console.WriteLine(" Options:");
            Console.WriteLine(" -cf  Create SQL files for missing tables and columns.");
        }
    }
}