using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DbSync.Structure
{
    public class Default : DbObject
    {
        public int ParentColumnId { get; set; }
        public string Definition { get; set; }

        internal Column Column { get; set; }

        internal override string CreateSql
        {
            get
            {
                return string.Format(
                    "IF NOT EXISTS (SELECT 1 FROM [sys].[default_constraints] " +
                    "WHERE [object_id] = OBJECT_ID(N'{1}') AND [parent_object_id] = OBJECT_ID(N'{0}')) " +
                    "ALTER TABLE [{0}] ADD CONSTRAINT [{1}]  DEFAULT {2} FOR [{3}]",
                    Column.Table.Name,
                    Name,
                    Definition,
                    Column.Name);
            }
        }

        internal override string AlterSql { get { return ""; } }
        internal override string DropSql
        {
            get
            {
                return string.Format(
                    "IF EXISTS (SELECT 1 FROM [sys].[default_constraints] " +
                    "WHERE [object_id] = OBJECT_ID(N'{1}') AND [parent_object_id] = OBJECT_ID(N'{0}')) " +
                    "ALTER TABLE [{0}] DROP CONSTRAINT [{1}]", Column.Table.Name, Name);
            }
        }

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var def = obj as Default;
            return Column.Name == def.Column.Name && Definition == def.Definition;
        }

        internal static void Create(IEnumerable<Table> tables, SqlConnection connect)
        {
            var list = new List<Default>();
            var tableids = tables.Select(t => t.ObjectId);
            using (var cmd = new SqlCommand(string.Format(
                "SELECT [name], [object_id], [parent_object_id], [parent_column_id], [definition] " +
                "FROM [sys].[default_constraints] " +
                "WHERE [parent_object_id] IN ({0})", string.Join(", ", tableids)),
                connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var table = tables.Single(t => t.ObjectId == reader.GetInt32(2));
                        int colid = reader.GetInt32(3);
                        var column = table.Columns.Single(c => c.ColumnId == colid);
                        var definstance = new Default
                        {
                            Name = reader.GetString(0),
                            ObjectId = reader.GetInt32(1),
                            ParentObjectId = table.ObjectId,
                            ParentColumnId = colid,
                            Column = column,
                            Definition = reader.GetString(4)
                        };
                        column.Default = definstance;
                        list.Add(definstance);
                    }
                }
            }

            foreach (var table in tables)
                table.Defaults = list.Where(d => d.Column.Table == table).ToArray();
        }
    }
}
