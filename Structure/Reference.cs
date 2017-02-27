using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DbSync.Structure
{
    public class Reference
    {
        public int ColumnId { get; set; }
        public int ParentColumnId { get; set; }
        public int ReferenceColumnId { get; set; }
        internal Column ParentColumn { get; set; }
        internal Column ReferenceColumn { get; set; }

        internal static Reference[] GetFromDb(ForeignKey fkey, SqlConnection connect)
        {
            var list = new List<Reference>();
            using (var cmd = new SqlCommand(
                "SELECT [constraint_column_id], [parent_column_id], [referenced_column_id]" +
                "FROM [sys].[foreign_key_columns] " +
                "WHERE [constraint_object_id] = " + fkey.ObjectId +
                " AND [parent_object_id] = " + fkey.ParentTable.ObjectId +
                " AND [referenced_object_id] = " + fkey.ReferenceTable.ObjectId, connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        int pcid = reader.GetInt32(1);
                        int rcid = reader.GetInt32(2);
                        list.Add(new Reference
                        {
                            ColumnId = reader.GetInt32(0),
                            ParentColumnId = pcid,
                            ReferenceColumnId = rcid,
                            ParentColumn = fkey.ParentTable.Columns.Single(c => c.ColumnId == pcid),
                            ReferenceColumn = fkey.ReferenceTable.Columns.Single(c => c.ColumnId == rcid)
                        });
                    }
                }
            }

            return list.ToArray();
        }
    }
}
