using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SSqlBackup.Models
{
    public class ColumnInfo
    {
        public bool IsPrimaryKey { get; set; }
        public IdentityInfo Identity { get; set; }
        public string Name { get; set; }
        public bool NullAble { get; set; }
        public string DataType { get; set; }
        public int MaxLength { get; set; }
    }
}
