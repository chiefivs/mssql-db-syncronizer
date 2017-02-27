using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DbSync.Structure
{
    public class IndexColumn
    {
        public int TableColumnId { get; set; }
        public int KeyOrdinal { get; set; }
        public bool IsDescendingKey { get; set; }
        internal Column TableColumn { get; set; }

        //private int _objectId;
        private int _indexId;

        internal static void Create(IEnumerable<Index> indexes, SqlConnection connect)
        {
            var list = new List<IndexColumn>();
            var tableids = indexes.Select(i => i.Table.ObjectId).Distinct();
            using(var cmd = new SqlCommand(string.Format(
                "SELECT [column_id], [key_ordinal], [is_descending_key], [object_id], [index_id] " +
                "FROM [sys].[index_columns] WHERE [object_id] IN ({0})", string.Join(", ", tableids)), connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        int indexid = reader.GetInt32(4);
                        var index = indexes.Single(i => i.Table.ObjectId == reader.GetInt32(3) && i.IndexId == indexid);
                        int colid = reader.GetInt32(0);
                        var col = index.Table.Columns.Single(c => c.ColumnId == colid);
                        list.Add(new IndexColumn
                        {
                            TableColumnId = colid,
                            TableColumn = col,
                            KeyOrdinal = reader.GetByte(1),
                            IsDescendingKey = reader.GetBoolean(2),
                            //_objectId = reader.GetInt32(3),
                            _indexId = reader.GetInt32(4)
                        });
                    }
                }
            }

            foreach (var index in indexes)
                index.Columns = list.Where(ic => ic.TableColumn.Table == index.Table && ic._indexId == index.IndexId).ToArray();
        }
    }
}
