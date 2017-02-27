using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Xml.Serialization;

namespace DbSync.Structure
{
    public class Column
    {
        [XmlAttribute]
        public string Name { get; set; }
        public int ColumnId { get; set; }
        public string Type { get; set; }
        public int MaxLength { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsComputed { get; set; }
        public string Definition { get; set; }
        internal Table Table { get; set; }
        internal Default Default { get; set; }

        internal string DropSql { get { return string.Format("ALTER TABLE [{0}] DROP COLUMN [{1}]", Table.Name, Name); } }
        internal string AlterAddSql { get { return string.Format("ALTER TABLE [{0}] ADD {1}", Table.Name, ParamsSql); } }
        internal string AlterModifySql { get { return string.Format("ALTER TABLE [{0}] ALTER COLUMN {1}", Table.Name, ParamsSql); } }
        internal string ParamsSql
        {
            get
            {
                if (IsComputed)
                    return string.Format("[{0}] AS {1}", Name, Definition);

                string type = Type.EndsWith("char")
                    ? string.Format("[{0}]({1})", Type, MaxLength > 0 ? (MaxLength / 2).ToString(CultureInfo.InvariantCulture) : "MAX")
                    : string.Format("[{0}]", Type);
                return string.Format("[{0}] {1} {2} {3} {4}",
                    Name,
                    type,
                    IsIdentity ? "IDENTITY(1,1)" : "",
                    IsNullable ? "NULL" : "NOT NULL",
                    Default != null ? string.Format("CONSTRAINT [{0}] DEFAULT {1}", Default.Name, Default.Definition) : "");
            }
        }

        //private int _objectId;

        // ReSharper disable once CSharpWarnings::CS0659
        public override bool Equals(object obj)
        {
            var col = obj as Column;
            return Type == col.Type
                && MaxLength == col.MaxLength
                && IsNullable == col.IsNullable
                && IsIdentity == col.IsIdentity
                && IsComputed == col.IsComputed;
        }

        internal static void Create(IEnumerable<Table> tables, SqlConnection connect)
        {
            if (!tables.Any())
                return;

            var list = new List<Column>();
            var tableids = tables.Select(t => t.ObjectId);
            using (var cmd = new SqlCommand(string.Format(
                "SELECT c.[object_id], c.[name], c.[column_id], t.[name], c.[max_length], c.[is_nullable], c.[is_identity], c.is_computed, cc.definition " +
                "FROM [sys].[columns] c " +
                "JOIN [sys].[types] t on c.[user_type_id]=t.[user_type_id] " +
                "LEFT JOIN [sys].[computed_columns] cc ON c.[column_id]=cc.[column_id] AND c.[object_id]=cc.[object_id] " +
                "WHERE c.[object_id] IN ({0})", string.Join(", ", tableids)),
                connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        list.Add(new Column
                        {
                            Table = tables.Single(t => t.ObjectId == reader.GetInt32(0)),
                            //_objectId = ,
                            Name = reader.GetString(1),
                            ColumnId = reader.GetInt32(2),
                            Type = reader.GetString(3),
                            MaxLength = reader.GetInt16(4),
                            IsNullable = reader.GetBoolean(5),
                            IsIdentity = reader.GetBoolean(6),
                            IsComputed = reader.GetBoolean(7),
                            Definition = reader.GetValue(8) as string
                        });
                    }
                }
            }

            foreach (var table in tables)
                table.Columns = list.Where(col => col.Table == table).ToArray();
        }
    }
}
