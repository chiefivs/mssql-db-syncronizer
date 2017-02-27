using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DbSync.Structure
{
    public class Table : DbObject
    {
        private readonly List<Table> _parentTables = new List<Table>(); 

        public Column[] Columns { get; set; }
        public Index[] Indexes { get; set; }
        public PrimaryKey PrimaryKey { get; set; }
        public  ForeignKey[] ForeignKeys { get; set; }
        public Default[] Defaults { get; set; }

        internal Table[] ParentTables {get { return _parentTables.ToArray(); }}
        internal void AddParentTable(Table table)
        {
            _parentTables.Add(table);
        }

        internal override string AlterSql { get { return ""; } }
        internal override string DropSql { get { return string.Format("DROP TABLE [{0}]", Name); } }
        internal override string CreateSql
        {
            get
            {
                var fields = new List<string>();
                fields.AddRange(Columns.Select(c => c.ParamsSql));

                if (PrimaryKey != null)
                    fields.Add(PrimaryKey.ParamsSql);

                return string.Format("CREATE TABLE [{0}]({1}) ON [PRIMARY]", Name, string.Join(", ", fields));
            }
        }

        internal string[] SyncronizeWith(Table source)
        {
            #region УДАЛЕНИЕ
            //  удаляем foreign keys, которых нет в source
            var existSourceFkeyNames = source.ForeignKeys.Select(k => k.Name);
            var delTargetFkeys = ForeignKeys.Where(k => !existSourceFkeyNames.Contains(k.Name));
            var list = delTargetFkeys.Select(fkey => fkey.DropSql).ToList();

            if(PrimaryKey != null && source.PrimaryKey == null)
                list.Add(PrimaryKey.DropSql);

            //  удаляем индексы, которых нет в source
            var existSourceIndexNames = source.Indexes.Select(k => k.Name);
            var delTargetIndexes = Indexes.Where(k => !k.IsPrimaryKey && !existSourceIndexNames.Contains(k.Name));
            list.AddRange(delTargetIndexes.Select(index => index.DropSql));

            //  удаляем дефолты, которых нет в source
            var existSourceDefNames = source.Defaults.Select(d => d.Name);
            var delTargetDefs = Defaults.Where(d => !existSourceDefNames.Contains(d.Name));
            list.AddRange(delTargetDefs.Select(def => def.DropSql));

            //  удаляем колонки, которых нет в source
            var existSourceColNames = source.Columns.Select(c => c.Name);
            var delTargetCols = Columns.Where(c => !existSourceColNames.Contains(c.Name));
            list.AddRange(delTargetCols.Select(col => col.DropSql));
            #endregion

            #region МОДИФИКАЦИЯ

            var existTargetColNames = Columns.Select(c => c.Name);
            var existSourceCols = source.Columns.Where(c => existTargetColNames.Contains(c.Name));
            foreach (var sourceCol in existSourceCols)
            {
                var targetCol = Columns.Single(c => c.Name == sourceCol.Name);
                if (!targetCol.Equals(sourceCol))
                {
                    var indexes = Indexes.Where(i => i.Columns.Any(ic => ic.TableColumn == targetCol));
                    list.AddRange(indexes.Select(index => index.DropSql));
                    list.Add(sourceCol.AlterModifySql);
                    list.AddRange(indexes.Select(index => index.CreateSql));
                }
            }

            var existTargetIndexNames = Indexes.Select(i => i.Name);
            var existSourceIndexes = source.Indexes.Where(i => existTargetIndexNames.Contains(i.Name));
            foreach (var sourceIndex in existSourceIndexes)
            {
                var targetIndex = Indexes.Single(i => i.Name == sourceIndex.Name);
                if (targetIndex.Equals(sourceIndex))
                    continue;

                list.Add(targetIndex.DropSql);
                list.Add(sourceIndex.CreateSql);
            }

            var existTargetDefNames = Defaults.Select(d => d.Name);
            var existSourceDefs = source.Defaults.Where(d => existTargetDefNames.Contains(d.Name));
            foreach (var sourceDef in existSourceDefs)
            {
                var targetDef = Defaults.Single(d => d.Name == sourceDef.Name);
                if (targetDef.Equals(sourceDef))
                    continue;

                list.Add(targetDef.DropSql);
                list.Add(sourceDef.CreateSql);
            }

            var existTargetFkeyNames = ForeignKeys.Select(k => k.Name);
            var existSourceFkeys = source.ForeignKeys.Where(k => existTargetFkeyNames.Contains(k.Name));
            foreach (var sourceFkey in existSourceFkeys)
            {
                var targetFkey = ForeignKeys.Single(k => k.Name == sourceFkey.Name);
                if(!targetFkey.Equals(sourceFkey))
                {
                    list.Add(targetFkey.DropSql);
                    list.Add(sourceFkey.CreateSql);
                    //list.Add(sourceFkey.AlterSql);
                }
            }

            if(PrimaryKey != null && source.PrimaryKey != null && !PrimaryKey.Equals(source.PrimaryKey))
                list.Add(source.PrimaryKey.AlterSql);

            #endregion

            #region ДОБАВЛЕНИЕ
            //  добавляем колонки, которых нет в target
            var addSourceCols = source.Columns.Where(c => !existTargetColNames.Contains(c.Name));
            list.AddRange(addSourceCols.Select(col => col.AlterAddSql));

            var addSourceDefs = source.Defaults.Where(d => !existTargetDefNames.Contains(d.Name));
            list.AddRange(addSourceDefs.Select(d => d.CreateSql));

            var addSourceFkeys = source.ForeignKeys.Where(k => !existTargetFkeyNames.Contains(k.Name));
            list.AddRange(addSourceFkeys.Select(key => key.CreateSql));

            if(PrimaryKey == null && source.PrimaryKey != null)
                list.Add(source.PrimaryKey.CreateSql);

            //  добавляем индексы, которых нет в target
            var addSourceIndexes = source.Indexes.Where(i => !i.IsPrimaryKey && !existTargetIndexNames.Contains(i.Name));
            list.AddRange(addSourceIndexes.Select(index => index.CreateSql));
            #endregion

            return list.ToArray();
        }

        internal static Table[] Create(SqlConnection connect)
        {
            var list = new List<Table>();
            using(var cmd = new SqlCommand("SELECT [name], [object_id] FROM [sys].[tables]", connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        int objid = reader.GetInt32(1);
                        list.Add(new Table{ Name = name, ObjectId = objid });
                    }
                }
            }

            Column.Create(list, connect);
            Index.Create(list, connect);
            Default.Create(list, connect);
            PrimaryKey.Create(list, connect);
            ForeignKey.CreateReferences(list, connect);

            return list.ToArray();
        }

        internal static Table[] Sort(IEnumerable<Table> tables)
        {
            var list = new List<Table>();
            foreach (var table in tables)
                AddRecursive(list, table);

            return list.ToArray();
        }

        private static void AddRecursive(ICollection<Table> list, Table table)
        {
            foreach (var pt in table.ForeignKeys.Where(r => r.ReferenceTable != table).Select(r => r.ReferenceTable))
                AddRecursive(list, pt);

            if (list.Contains(table))
                return;

            list.Add(table);
        }
    }
}
