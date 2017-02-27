using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace DbSync.Structure
{
    public class View : ScriptObject
    {
        internal override string CreateSql { get { return Definition; } }
        internal override string AlterSql { get { return Definition.Replace("CREATE", "ALTER"); } }
        internal override string DropSql { get { return string.Format("DROP VIEW [{0}]", Name); } }

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var view = obj as View;
            string norm1 = Regex.Replace(Definition, @"\s", "");
            string norm2 = Regex.Replace(view.Definition, @"\s", "");

            return norm1 == norm2;
        }

        internal static View[] Create(SqlConnection connect)
        {
            var list = new List<View>();
            using (var cmd = new SqlCommand("SELECT [name], [object_id], OBJECT_DEFINITION([object_id]) FROM [sys].[views]", connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        list.Add(new View
                        {
                            Name = reader.GetString(0),
                            ObjectId = reader.GetInt32(1),
                            Definition = reader.GetString(2)
                        });
                    }
                }
            }

            return list.ToArray();
        }
    }
}
