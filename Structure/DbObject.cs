using System.Xml.Serialization;

namespace DbSync.Structure
{
    public abstract class DbObject
    {
        [XmlAttribute]
        public string Name { get; set; }

        [XmlAttribute]
        public int ObjectId { get; set; }

        [XmlAttribute]
        public int ParentObjectId { get; set; }

        internal abstract string CreateSql { get; }
        internal abstract string AlterSql { get; }
        internal abstract string DropSql { get; }
    }
}
