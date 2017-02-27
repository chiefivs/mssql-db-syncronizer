using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Xml.Serialization;

namespace DbSync.Structure
{
    public class Index
    {
        [XmlAttribute]
        public string Name { get; set; }
        public int IndexId { get; set; }
        public string Type { get; set; }
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsUniqueConstraint { get; set; }
        public IndexColumn[] Columns { get; set; }
        internal Table Table { get; set; }

        internal string DropSql { get { return string.Format("DROP INDEX [{0}] ON [{1}]", Name, Table.Name); } }
        internal string CreateSql
        {
            get
            {
                var cols = Columns
                    .OrderBy(c => c.KeyOrdinal)
                    .Select(c => string.Format("[{0}] {1}", c.TableColumn.Name, c.IsDescendingKey ? "DESC" : "ASC"));

                return string.Format("CREATE {0} {1} INDEX [{2}] ON [{3}] ({4})",
                    IsUnique ? "UNIQUE" : "",
                    Type,
                    Name,
                    Table.Name,
                    string.Join(", ", cols)
                    );
            }
        }

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var index = obj as Index;
            bool isColsEqual = true;
            if (Columns.Length == index.Columns.Length)
            {
                foreach (var col in Columns)
                {
                    if (index.Columns.Any(ic =>
                        col.TableColumn.Name == ic.TableColumn.Name
                        && col.IsDescendingKey == ic.IsDescendingKey
                        && col.KeyOrdinal == ic.KeyOrdinal))
                        continue;

                    isColsEqual = false;
                    break;
                }
            }
            else
                isColsEqual = false;

            return Type == index.Type && IsUnique == index.IsUnique && isColsEqual;
        }

        internal static void Create(IEnumerable<Table> tables, SqlConnection connect)
        {
            var list = new List<Index>();
            var tableids = tables.Select(t => t.ObjectId);
            using(var cmd = new SqlCommand(string.Format(
                "SELECT [object_id], [index_id], [name], [type_desc], [is_unique], [is_primary_key], [is_unique_constraint] " +
                "FROM [sys].[indexes] WHERE [name] IS NOT NULL AND [object_id] IN ({0})", string.Join(", ", tableids)), connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        int tableid = reader.GetInt32(0);
                        list.Add(new Index
                        {
                            //_objectId = ,
                            IndexId = reader.GetInt32(1),
                            Name = reader.GetString(2),
                            Type = reader.GetString(3),
                            IsUnique = reader.GetBoolean(4),
                            IsPrimaryKey = reader.GetBoolean(5),
                            IsUniqueConstraint = reader.GetBoolean(6),
                            Table = tables.Single(t => t.ObjectId == tableid)
                        });
                    }
                }
            }

            foreach (var table in tables)
                table.Indexes = list.Where(index => index.Table == table).ToArray();

            IndexColumn.Create(list, connect);
        }
    }
}
