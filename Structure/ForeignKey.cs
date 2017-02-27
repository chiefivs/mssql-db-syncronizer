using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DbSync.Structure
{
    public class ForeignKey : DbObject
    {
        public int ReferenceTableId { get; set; }
        public Reference[] References { get; set; }
        public string DeleteAction { get; set; }
        public string UpdateAction { get; set; }
        internal Table ParentTable { get; set; }
        internal Table ReferenceTable { get; set; }
        internal bool IsDeleted { get; set; }

        internal override string DropSql { get { return string.Format(
            "IF EXISTS (SELECT 1 FROM [sys].[foreign_keys] " +
            "WHERE [object_id] = OBJECT_ID(N'{1}') AND [parent_object_id] = OBJECT_ID(N'{0}')) " +
            "ALTER TABLE [{0}] DROP CONSTRAINT [{1}]",
            ParentTable.Name, Name); } }
        internal override string CreateSql { get { return string.Format("ALTER TABLE [{0}] ADD {1}", ParentTable.Name, ParamsSql); } }
        internal override string AlterSql { get { return string.Format("ALTER TABLE [{0}] MODIFY {1}", ParentTable.Name, ParamsSql); } }
        internal string ParamsSql
        {
            get
            {
                return string.Format(
                    "CONSTRAINT [{0}] FOREIGN KEY ({1}) REFERENCES [{2}]({3}) {4} {5}",
                    Name,
                    string.Join(", ", References.Select(r => string.Format("[{0}]", r.ParentColumn.Name))),
                    ReferenceTable.Name,
                    string.Join(", ", References.Select(r => string.Format("[{0}]", r.ReferenceColumn.Name))),
                    DeleteAction != "NO_ACTION" ? "ON DELETE " + DeleteAction.Replace('_', ' ') : "",
                    UpdateAction != "NO_ACTION" ? "ON UPDATE " + UpdateAction.Replace('_', ' ') : ""
                );

            }
        }

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var fkey = obj as ForeignKey;
            bool isRefEqual = true;
            if (References.Length == fkey.References.Length)
            {
                for (int n = 0; n < References.Length; n++)
                {
                    if (References[n].ParentColumn.Name == fkey.References[n].ParentColumn.Name
                        && References[n].ReferenceColumn.Name == fkey.References[n].ReferenceColumn.Name)
                        continue;

                    isRefEqual = false;
                    break;
                }
            }
            else
                isRefEqual = false;

            return ReferenceTable.Name == fkey.ReferenceTable.Name
                   && DeleteAction == fkey.DeleteAction
                   && UpdateAction == fkey.UpdateAction
                   && isRefEqual;
        }

        internal static void CreateReferences(IEnumerable<Table> tables, SqlConnection connect)
        {
            foreach (var table in tables)
            {
                using(var cmd = new SqlCommand(
                    "SELECT [object_id], [name], [referenced_object_id], [delete_referential_action_desc], [update_referential_action_desc] " +
                    "FROM [sys].[foreign_keys] " +
                    "WHERE [parent_object_id] = " + table.ObjectId, connect))
                using (var reader = cmd.ExecuteReader())
                {
                    var list = new List<ForeignKey>();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            int refid = reader.GetInt32(2);
                            var reftable = tables.Single(t => t.ObjectId == refid);
                            reftable.AddParentTable(table);
                            list.Add(new ForeignKey
                            {
                                ObjectId = reader.GetInt32(0),
                                ParentObjectId = table.ObjectId,
                                Name = reader.GetString(1),
                                DeleteAction = reader.GetString(3),
                                UpdateAction = reader.GetString(4),
                                ParentTable = table,
                                ReferenceTableId = refid,
                                ReferenceTable = reftable
                            });
                        }
                    }

                    table.ForeignKeys = list.ToArray();
                }

                foreach (var fkey in table.ForeignKeys)
                {
                    fkey.References = Reference.GetFromDb(fkey, connect);
                }
            }
        }
    }
}
