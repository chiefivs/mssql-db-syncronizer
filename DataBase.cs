using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using DbSync.Structure;

namespace DbSync
{
    public class DataBase : IDisposable
    {
        private readonly SqlConnection _connect;
        private SqlTransaction _transaction;

        public int Version { get; set; }
        public Table[] Tables { get; set; }
        public View[] Views { get; set; }
        public ProgObject[] ProgrammedObjects { get; set; }

        public DataBase()
        {
            Version = 0;
        }

        internal DataBase(string connString)
        {
            _connect = new SqlConnection(connString);
            _connect.Open();

            Tables = Table.Create(_connect);
            //Tables = Table.Sort(Tables);

            Views = View.Create(_connect);
            ProgrammedObjects = ProgObject.Create(_connect);

            Version = GetVersion();
        }

        internal string[] SyncronizeWith(DataBase source)
        {
            var list = new List<string>();

            #region TABLES
            //  удаляем таблицы, которых нет в source
            var existSourceTableNames = source.Tables.Select(t => t.Name);
            var delTargetTables = Tables.Where(t => !existSourceTableNames.Contains(t.Name));
            var delTargetFkeys =
                delTargetTables.SelectMany(
                    t => t.ParentTables.SelectMany(pt => pt.ForeignKeys.Where(fk => fk.ReferenceTable == t)));
            list.AddRange(delTargetFkeys.Select(fk => fk.DropSql));
            list.AddRange(delTargetTables.Select(table => table.DropSql));

            //  добавляем новые таблицы, индексы и ключи
            var existTargetTableNames = Tables.Select(t => t.Name);
            var addSourceTables = source.Tables.Where(t => !existTargetTableNames.Contains(t.Name));
            list.AddRange(addSourceTables.Select(table => table.CreateSql));
            list.AddRange(addSourceTables.SelectMany(table => table.Defaults.Select(d => d.CreateSql)));
            list.AddRange(addSourceTables.SelectMany(table => table.ForeignKeys.Select(fkey => fkey.CreateSql)));
            list.AddRange(addSourceTables.SelectMany(table => table.Indexes.Where(index => !index.IsPrimaryKey).Select(index => index.CreateSql)));

            //  проверяем на наличие изменений и модифицируем существующие таблицы
            var existSourceTables = source.Tables.Where(t => existTargetTableNames.Contains(t.Name));
            foreach (var sourceTable in existSourceTables)
            {
                var targetTable = Tables.Single(t => t.Name == sourceTable.Name);
                list.AddRange(targetTable.SyncronizeWith(sourceTable));
            }
            #endregion

            #region VIEWS &  PROGRAMMED OBJECTS
            //  удаление
            var existSourceViewNames = source.Views.Select(view => view.Name);
            var delTargetViews = Views.Where(view => !existSourceViewNames.Contains(view.Name));
            list.AddRange(delTargetViews.Select(view => view.DropSql));
            var existSourceObjNames = source.ProgrammedObjects.Select(pobj => pobj.Name);
            var delTargetObjs = ProgrammedObjects.Where(pobj => !existSourceObjNames.Contains(pobj.Name));
            list.AddRange(delTargetObjs.Select(pobj => pobj.DropSql));

            //  объекты, которые нужно добавить или модифицировать
            var sourceViews = source.Views
                .Select(sview =>
                {
                    var tview = Views.SingleOrDefault(sv => sv.Name == sview.Name);
                    return new SourceScript
                    {
                        IsNew = tview == null,
                        IsChanged = tview != null && !sview.Equals(tview),
                        Instance = sview
                    };
                });

            var sourceObjs = source.ProgrammedObjects
                .Select(sobj =>
                {
                    var tobj = ProgrammedObjects.SingleOrDefault(sv => sv.Name == sobj.Name);
                    return new SourceScript
                    {
                        IsNew = tobj == null,
                        IsChanged = tobj != null && !sobj.Equals(tobj),
                        Instance = sobj
                    };
                });

            var sourceScripts = SourceScript.Sort(sourceViews.Union(sourceObjs).Where(i => i.IsNew || i.IsChanged));
            list.AddRange(sourceScripts.Select(tscript => tscript.IsNew ? tscript.Instance.CreateSql : tscript.Instance.AlterSql));

            /*
                        //  добавление
                        var addSourceViews = source.Views.Where(view => !existTargetViewNames.Contains(view.Name));
                        list.AddRange(addSourceViews.Select(view => view.CreateSql));
                        var addSourceObjs = source.ProgrammedObjects.Where(pobj => !existTargetObjNames.Contains(pobj.Name));
                        list.AddRange(addSourceObjs.Select(view => view.CreateSql));

                        //  модификация
                        var existSourceViews = source.Views.Where(view => existTargetViewNames.Contains(view.Name));
                        foreach (var sourceView in existSourceViews)
                        {
                            var targetView = Views.Single(view => view.Name == sourceView.Name);
                            if (!targetView.Equals(sourceView))
                                list.Add(sourceView.AlterSql);
                        }
                        var existSourceObjs = source.ProgrammedObjects.Where(pobj => existTargetObjNames.Contains(pobj.Name));
                        foreach (var sourceObj in existSourceObjs)
                        {
                            var targetObj = ProgrammedObjects.Single(pobj => pobj.Name == sourceObj.Name);
                            if (!targetObj.Equals(sourceObj))
                                list.Add(sourceObj.AlterSql);
                        }
            */
            #endregion

            #region PROGRAMMED OBJECTS
            //  удаление

            //  добавление

            //  модификация
            #endregion

            return list.ToArray();
        }

        private class SourceScript
        {
            public bool IsNew;
            public bool IsChanged;
            public ScriptObject Instance;

            public static IEnumerable<SourceScript> Sort(IEnumerable<SourceScript> list)
            {
                var res = new List<SourceScript>();
                foreach (var item in list)
                    AddRecursive(res, item, list);

                return res;
            }

            private static void AddRecursive(List<SourceScript> target, SourceScript item, IEnumerable<SourceScript> source)
            {
                if (target.Any(i => i.Instance.Name == item.Instance.Name))
                    return;

                var dependencies = source.Where(src => src.Instance.Name != item.Instance.Name && item.Instance.Definition.Contains(src.Instance.Name));
                foreach (var dependency in dependencies)
                    AddRecursive(target, dependency, source);

                target.Add(item);
            }
        }

        internal void BeginTransaction()
        {
            _transaction = _connect.BeginTransaction();
        }

        internal void Commit()
        {
            if (_transaction == null)
                return;

            _transaction.Commit();
            _transaction = null;
        }

        internal void Rollback()
        {
            if (_transaction == null)
                return;

            _transaction.Rollback();
            _transaction = null;
        }

        internal void Execute(string sql)
        {
            var cmd = new SqlCommand(sql, _connect);
            if (_transaction != null)
                cmd.Transaction = _transaction;
            cmd.ExecuteNonQuery();
        }

        public void Dispose()
        {
            if (_connect != null)
                _connect.Dispose();
        }

        internal string GetXml()
        {
            var xs = new XmlSerializer(GetType());
            using (var ms = new MemoryStream())
            using (var sr = new StreamReader(ms))
            {
                xs.Serialize(ms, this);
                ms.Seek(0, SeekOrigin.Begin);
                return sr.ReadToEnd();
            }
        }

        internal static DataBase CreateFromXml(string xml)
        {
            var xs = new XmlSerializer(typeof(DataBase));
            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms))
            {
                sw.Write(xml);
                sw.Flush();
                ms.Seek(0, SeekOrigin.Begin);
                var db = xs.Deserialize(ms) as DataBase;

                foreach (var table in db.Tables)
                {
                    foreach (var col in table.Columns)
                        col.Table = table;

                    foreach (var index in table.Indexes)
                    {
                        index.Table = table;
                        foreach (var icol in index.Columns)
                            icol.TableColumn = table.Columns.Single(c => c.ColumnId == icol.TableColumnId);
                    }

                    if (table.PrimaryKey != null)
                    {
                        table.PrimaryKey.UniqueIndex = table.Indexes.Single(i => i.IndexId == table.PrimaryKey.UniqueIndexId);
                        table.PrimaryKey.Table = table;
                    }

                    foreach (var def in table.Defaults)
                    {
                        def.Column = table.Columns.Single(c => c.ColumnId == def.ParentColumnId);
                        def.Column.Default = def;
                    }

                    foreach (var fkey in table.ForeignKeys)
                    {
                        fkey.ParentTable = table;
                        fkey.ReferenceTable = db.Tables.Single(t => t.ObjectId == fkey.ReferenceTableId);
                        fkey.ReferenceTable.AddParentTable(table);
                        foreach (var refer in fkey.References)
                        {
                            refer.ParentColumn = table.Columns.Single(c => c.ColumnId == refer.ParentColumnId);
                            refer.ReferenceColumn = fkey.ReferenceTable.Columns.Single(c => c.ColumnId == refer.ReferenceColumnId);
                        }
                    }
                }

                return db;
            }
        }

        /// <summary>
        /// Синхронизирует структуру базы данных с описанной в XML-файле.
        /// Если версия базы данных больше, чем файла, обновляется XML-файл,
        /// если меньше, обновляется структура БД.
        /// В случае равенства версий ничего не происходит.
        /// </summary>
        /// <param name="connString">
        /// Строка коннекта к базе данных.
        /// </param>
        /// <param name="xmlFilePath">
        /// Путь к XML-файлу с описанием структуры БД
        /// </param>
        /// <param name="logDirPath">
        /// Путь к каталогу логов
        /// </param>
        /// <param name="sqlScriptPath">
        /// Путь к файлу SQL-скрипта для обновления базы (необяз.)
        /// В случае указания при обновлении структуры БД сюда выводится скрипт
        /// </param>
        public static SyncronizeResult Syncronize(string connString, string xmlFilePath, string logDirPath, string sqlScriptPath = null)
        {
            var result = new SyncronizeResult();
            if (!Directory.Exists(logDirPath))
                Directory.CreateDirectory(logDirPath);

            string logFilePath = Path.Combine(logDirPath, string.Format("db_update_{0:yyyy.MM.dd_HH.mm}.log", DateTime.Now));
            //using (var logStream = File.Open(logFilePath, FileMode.Create))
            //using (var logWriter = new StreamWriter(logStream))
            {
                try
                {
                    using (var db = new DataBase(connString))
                    {
                        var xml = File.Exists(xmlFilePath)
                            ? CreateFromXml(File.ReadAllText(xmlFilePath))
                            : new DataBase();

                        result.DbVersion = db.Version;
                        result.XmlVersion = xml.Version;

                        if (db.Version > xml.Version)
                        {
                            File.WriteAllText(xmlFilePath, db.GetXml());
                            File.AppendAllText(logFilePath, "Xml file updated to Version " + db.Version + Environment.NewLine);
                        }
                        else if (db.Version < xml.Version)
                        {
                            var sqlLines = db.SyncronizeWith(xml);
                            if (sqlScriptPath != null)
                            {
                                File.WriteAllLines(sqlScriptPath, sqlLines);
                            }

                            db.BeginTransaction();
                            bool isRollback = false;
                            foreach (var sql in sqlLines)
                            {
                                try
                                {
                                    File.AppendAllText(logFilePath, sql + Environment.NewLine);
                                    db.Execute(sql);
                                    File.AppendAllText(logFilePath, "/* SUCCESS!!! */" + Environment.NewLine);
                                }
                                catch (Exception ex)
                                {
                                    File.AppendAllLines(logFilePath, new[]{
                                        "/* SQL EXCEPTION!!!" + Environment.NewLine,
                                        "Message: " + ex.Message + Environment.NewLine,
                                        "Source: " + ex.Source + Environment.NewLine,
                                        "StackTrace: " + ex.StackTrace + Environment.NewLine,
                                        "*/" + Environment.NewLine});
                                    isRollback = true;
                                    break;
                                }
                            }

                            if (!isRollback)
                            {
                                db.Commit();
                                db.SetVersion(xml.Version);
                            }
                            else
                                db.Rollback();
                        }
                    }
                }
                catch (Exception ex)
                {
                    File.AppendAllLines(logFilePath, new[]{
                    "/* GLOBAL EXCEPTION!!!" + Environment.NewLine,
                    "Message: " + ex.Message + Environment.NewLine,
                    "Source: " + ex.Source + Environment.NewLine,
                    "StackTrace: " + ex.StackTrace + Environment.NewLine,
                    "*/" + Environment.NewLine});
                }
            }

            return result;
        }

        public class SyncronizeResult
        {
            public int XmlVersion { get; set; }
            public int DbVersion { get; set; }
        }

        private bool HasVersion
        {
            get
            {
                using (var cmd = new SqlCommand(
                    "SELECT COUNT(1) " +
                    "FROM fn_listextendedproperty(default, default, default, default, default, default, default) " +
                    "WHERE [name] = 'Version'", _connect))
                {
                    return (int)cmd.ExecuteScalar() > 0;
                }
            }
        }

        private int GetVersion()
        {
            using (var cmd = new SqlCommand(
                "SELECT [value] " +
                "FROM fn_listextendedproperty(default, default, default, default, default, default, default) " +
                "WHERE [name] = 'Version'", _connect))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    try
                    {
                        reader.Read();
                        return int.Parse(reader.GetString(0));
                    }
                    catch
                    {
                        return 0;
                    }
                }

                return 0;
            }
        }

        private void SetVersion(int version)
        {
            using (var cmd = new SqlCommand(
                string.Format(HasVersion
                ? "EXEC sp_updateextendedproperty @name = N'Version', @value = '{0}'"
                : "sp_addextendedproperty @name = N'Version', @value = '{0}'", version), _connect))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
    //--select * from sys.extended_properties
    //--EXEC sp_addextendedproperty @name = N'Version', @value = '1.2'
    //--EXEC sp_updateextendedproperty @name = N'Version', @value = '1.2'
    /*
        type	type_desc
      * D 	DEFAULT_CONSTRAINT
      * F 	FOREIGN_KEY_CONSTRAINT
      + FN	SQL_SCALAR_FUNCTION
        FS	CLR_SCALAR_FUNCTION
        IT	INTERNAL_TABLE
      + P 	SQL_STORED_PROCEDURE
      * PK	PRIMARY_KEY_CONSTRAINT
        S 	SYSTEM_TABLE
        SQ	SERVICE_QUEUE
      + TF	SQL_TABLE_VALUED_FUNCTION
      + TR	SQL_TRIGGER
      * U 	USER_TABLE
      * V 	VIEW
     */
}
