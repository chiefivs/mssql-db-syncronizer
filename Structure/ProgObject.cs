using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace DbSync.Structure
{
    public class ProgObject : ScriptObject
    {
        public string Type { get; set; }

        internal override string CreateSql { get { return Definition; } }
        internal override string AlterSql { get { return "ALTER" + Definition.Substring(6); } }
        internal override string DropSql { get { return string.Format(
            "IF EXISTS (SELECT 1 FROM [sys].[objects] " +
            "WHERE [object_id] = OBJECT_ID(N'{1}') AND [type_desc] = N'{2}') " +
            "DROP {0} [{1}]"
            , Type.Split('_').Last(), Name, Type); } }

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var pobj = obj as ProgObject;
            string norm1 = Regex.Replace(Definition, @"\s", "");
            string norm2 = Regex.Replace(pobj.Definition, @"\s", "");

            return norm1 == norm2;
        }

        internal static ProgObject[] Create(SqlConnection connect)
        {
            var list = new List<ProgObject>();
            using (var cmd = new SqlCommand("SELECT [name], [object_id], [type_desc], OBJECT_DEFINITION([object_id]) " +
                "FROM [sys].[objects] " +
                "WHERE type IN ('TF', 'FN', 'P', 'TR')", connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var lines = new List<string>(reader.GetString(3).Split(new[] {'\r', '\n'}, StringSplitOptions.RemoveEmptyEntries));
                        while(lines.Count > 0 && !lines[0].StartsWith("CREATE"))
                            lines.RemoveAt(0);
                        //lines.Add("GO");

                        list.Add(new ProgObject
                        {
                            Name = reader.GetString(0),
                            ObjectId = reader.GetInt32(1),
                            Type = reader.GetString(2),
                            Definition = string.Join(Environment.NewLine, lines)
                        });
                    }
                }
            }

            return list.ToArray();
        }
    }
}
