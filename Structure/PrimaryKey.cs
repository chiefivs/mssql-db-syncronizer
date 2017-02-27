using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DbSync.Structure
{
    public class PrimaryKey : DbObject
    {
        public int UniqueIndexId { get; set; }
        internal Index UniqueIndex { get; set; }
        internal Table Table { get; set; }

        internal override string DropSql { get { return string.Format("ALTER TABLE [{0}] DROP CONSTRAINT [{1}]", Table.Name, Name); } }
        internal override string CreateSql { get { return string.Format("ALTER TABLE [{0}] ADD {1}", Table.Name, ParamsSql); } }
        internal override string AlterSql { get { return string.Format("ALTER TABLE [{0}] MODIFY {1}", Table.Name, ParamsSql); } }

        internal string ParamsSql
        {
            get
            {
                var cols = UniqueIndex.Columns
                    .OrderBy(c => c.KeyOrdinal)
                    .Select(c => string.Format("[{0}] {1}",
                        c.TableColumn.Name,
                        c.IsDescendingKey ? "DESC" : "ASC"));
                
                return string.Format("CONSTRAINT [{0}] PRIMARY KEY {1} ({2})" , 
                    Name, 
                    UniqueIndex.Type, 
                    string.Join(", ", cols));
            }
        }

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var pkey = obj as PrimaryKey;
            return UniqueIndex.Equals(pkey.UniqueIndex);
        }

        internal static PrimaryKey Create(IEnumerable<Table> tables, SqlConnection connect)
        {
            var tableids = tables.Select(t => t.ObjectId);
            using (var cmd = new SqlCommand(string.Format(
                "SELECT [object_id], [name], [unique_index_id], [parent_object_id] " + 
                "FROM [sys].[key_constraints] " + 
                "WHERE [parent_object_id] IN ({0})", string.Join(", ",tableids)), connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var table = tables.Single(t => t.ObjectId == reader.GetInt32(3));
                        int uiid = reader.GetInt32(2);
                        var uindex = table.Indexes.Single(i => i.IndexId == uiid);

                        table.PrimaryKey =  new PrimaryKey
                        {
                            ObjectId = reader.GetInt32(0),
                            Name = reader.GetString(1),
                            ParentObjectId = table.ObjectId,
                            UniqueIndexId = uiid,
                            UniqueIndex = uindex,
                            Table = table
                        };
                    }
                }
            }

            return null;
        }
    }
}
