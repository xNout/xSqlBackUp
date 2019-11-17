using SSqlBackup.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SSqlBackup
{
    public class xSqlBackUp
    {
        private SqlConnection Connection = null;
        private string DATABASE_NAME;
        // T-Sql scripts abreviados
        #region scripts_sql
        private readonly string PRIMARYKEY_PLANTILLA = "PRIMARY KEY CLUSTERED \n    ( \n        [{0}] ASC \n    )";
        private readonly string IDENTITY_PLANTILLA = "    CONSTRAINT [PK_dbo.{0}] ";
        private readonly string SELECT_DATABASE = "USE {0};";
        private readonly string DATABASE_PLANTILLA = "SET ANSI_NULLS ON \nSET QUOTED_IDENTIFIER ON \n\nCREATE TABLE [dbo].[{0}]( \n{1} \n) \n";
        private readonly string GET_COLUMNS_QUERY = "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}'";
        private readonly string GET_TABLE_PRIMARYKEY = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_NAME = '{0}'";
        private readonly string IS_IDENTITY_COLUMN = "SELECT * FROM SYS.IDENTITY_COLUMNS where object_id = OBJECT_ID( '{0}') AND name = '{1}'";
        private readonly string DROP_TABLE = "IF EXISTS (SELECT * FROM sysobjects WHERE name='{0}' and xtype='U') \nDROP TABLE {0} \n";
        private readonly string GET_DB_TABLES = "SELECT NAME FROM SYS.TABLES";
        private readonly string INSERT_VALUES_1 = "SET IDENTITY_INSERT {0} ON;\nINSERT INTO {0} ( {1} ) VALUES ";
        private readonly string INSERT_VALUES_2 = "SET IDENTITY_INSERT {0} OFF;\n";
        private readonly string SIMPLE_QUERY = "SELECT * FROM {0}";

        #endregion
        public void Open (string StringConnection, string DatabaseName )
        {
            Connection = new SqlConnection(StringConnection);
            Connection.Open();
            this.DATABASE_NAME = DatabaseName;
        }
        public async Task<bool> Restore( string Archivo )
        {
            if (Connection == null)
                throw new NotOpenSqlConnection();

            SelectDb(DATABASE_NAME);

            string SecuenciasSql = File.ReadAllText(Archivo, Encoding.UTF8);

            using (var SqlCmd = new SqlCommand(SecuenciasSql, Connection))
                await SqlCmd.ExecuteNonQueryAsync();

            return true;

        }
        public async Task<bool> BackUp( string Path )
        {
            if (Connection == null)
                throw new NotOpenSqlConnection();

            SelectDb(DATABASE_NAME);

            string Respaldo = string.Format(SELECT_DATABASE, DATABASE_NAME);
            
            foreach( string Tabla in await GetTables())
            {
                string Inicio = string.Format("/****** Object:  Table [dbo].[{0}]    Script Date: {1} ******/", Tabla, DateTime.Now);

                var Columnas = await GetColumns(Tabla);

                /* Concatenamos la plantilla con el cuerpo de la tabla generado desde el script: GetTableBody.
                 * El cuerpo que genera, es uno como el siguiente
                 * 
                 * [CompraID] [int] IDENTITY(1, 1),
                 * [Fecha] [datetime] NOT NULL,
                 * [Recibo] [nvarchar] (max) NULL,
                 * [ProveedorID] [int] NOT NULL,
                 * 
                 */
                string PLANTILLA_TABLA = string.Format(DATABASE_PLANTILLA, Tabla, GetTableBody(Columnas, Tabla));
                
                string VALORES = await GetTableValues(Tabla, Columnas);

                Respaldo += string.Format("\n{0}\n\n{1}\n\n{2}\n\n{3}\n", Inicio, DropIfExistsString(Tabla), PLANTILLA_TABLA, VALORES);
            }

            File.WriteAllText(Path, Respaldo);
            return true;
        }
        #region tablas
        async Task<List<string>> GetTables()
        {
            var Listado = new List<string>();

            using (SqlCommand CMD = new SqlCommand(GET_DB_TABLES, Connection))
            {
                var Reader = await Execute(CMD);

                while (await Reader.ReadAsync())
                    Listado.Add(Reader.GetString(0));
            }

            return Listado;
        }
        async Task<string> GetTableValues( string Tabla, List<ColumnInfo> Columnas )
        {
            string Valores = string.Format(INSERT_VALUES_1, Tabla, ConcatenarColumnas(Columnas));
            
            var ValoresTabla = await ExecuteQuery( string.Format(SIMPLE_QUERY, Tabla));

            if (ValoresTabla.Rows.Count == 0)
                return string.Format("/****** Object:  Table [dbo].[{0}]    0 ROWS ******/", Tabla);

            int rows_added = 1;
            foreach (DataRow Fila in ValoresTabla.Rows)
            {
                string Cache = "";

                int col_added = 0;

                foreach (ColumnInfo Columna in Columnas)
                {
                    // los tipo de datos como: int, decimal y bit son datos que no requieren ir almacenados dentro de cadenas de texto
                    string formato = Columna.DataType == "int" || Columna.DataType == "decimal" || Columna.DataType == "bit" ?
                        "{0}"
                    :
                        "'{0}'";

                    string ValorColumna = Fila[col_added].ToString();

                    // El formato de fecha en C# no es idéntico al del sql, por lo que se procede a aplicar un formato legible para la bd
                    if (Columna.DataType == "datetime")
                        ValorColumna = Convert.ToDateTime(ValorColumna).ToString("yyyy-MM-dd HH:mm:ss");

                    // los tipos 'decimal', dependiendo de la región utilizan el '.' para separar decimales.
                    // lo que en lenguaje sql se traduce como una separación y no como un número.
                    else if (Columna.DataType == "decimal")
                        ValorColumna = ValorColumna.Replace(',', '.');

                    if (ValorColumna == "True")
                        ValorColumna = "1";

                    else if (ValorColumna == "False")
                        ValorColumna = "0";

                    else if (string.IsNullOrEmpty(ValorColumna) || string.IsNullOrWhiteSpace(ValorColumna))
                        ValorColumna = "null";

                    string Valor = string.Format(formato, ValorColumna);

                    if (Cache == "")
                        Cache += " \n( " + Valor;
                    else
                        Cache += ", " + Valor;

                    col_added++;
                }

                // Esto ayuda a determinar cuál es el último resultado a agregar
                if (rows_added < ValoresTabla.Rows.Count)
                    Cache += "), ";
                else
                    Cache += "); ";

                rows_added++;

                Valores += Cache;
            }

            return Valores + "\n" + string.Format(INSERT_VALUES_2, Tabla);
        }
        string GetTableBody(List<ColumnInfo> Columnas, string Tabla)
        {
            string TableBody = "";
            int coladdeds = 0;
            foreach (ColumnInfo ColInfo in Columnas)
            {
                // Primero se fija, el nombre de la columna y luego el tipo de dato
                string Columna = "";

                if (coladdeds > 0)
                    Columna += ",\n";

                Columna += string.Format("    [{0}] [{1}] ", ColInfo.Name, ColInfo.DataType);


                // Después del tipo de dato fijamos otras cosas adicionales, como por ejemplo: Llaves primarias o identidades
                if (ColInfo.IsPrimaryKey)
                {
                    if (ColInfo.Identity != null)
                        Columna += string.Format("IDENTITY({0}, {1})", ColInfo.Identity.Seed, ColInfo.Identity.Increment);
                    else
                        Columna += "PRIMARY KEY";
                }
                else
                {
                    // Otros tipos como int, decimal, datetime, etc. No necesitan fijar una longitud especifica

                    if (ColInfo.DataType == "nvarchar" || ColInfo.DataType == "varbinary")
                        Columna += string.Format("({0}) ", ColInfo.MaxLength == -1 ? "max" : ColInfo.MaxLength.ToString());

                    if (ColInfo.NullAble)
                        Columna += "NULL";
                    else
                        Columna += "NOT NULL";

                }
                TableBody += Columna;
                coladdeds++;
            }

            // Una vez añadidas las columnas, nos fijamos si la tabla tiene o no una llave primaria ( o identidad )
            var ColumnKey = Columnas.Where(x => x.IsPrimaryKey == true).FirstOrDefault();

            if (ColumnKey != null)
            {
                TableBody += ",\n";

                // Para evitar escribir cadenas de texto muy extensas, se guardaron valores en campos privados para tener un código más limpio.
                if (ColumnKey.Identity != null)
                    TableBody += string.Format(IDENTITY_PLANTILLA, Tabla);

                TableBody += string.Format(PRIMARYKEY_PLANTILLA, ColumnKey.Name);
            }

            return TableBody;
        }
        #endregion
        #region columnas
        public async Task<List<ColumnInfo>> GetColumns( string Table )
        {
            var Cols = new List<ColumnInfo>();

            string CmdQuery = string.Format(GET_COLUMNS_QUERY, Table);

            using (SqlCommand CMD = new SqlCommand(CmdQuery, Connection))
            {
                var Reader = await Execute(CMD);

                while (await Reader.ReadAsync())
                {

                    var Modelo = new ColumnInfo
                    {
                        IsPrimaryKey = false,
                        Name = Reader.GetString(0),
                        NullAble = Reader.GetString(1) == "YES" ? true : false,
                        DataType = Reader.GetString(2),
                        MaxLength = Reader.IsDBNull(3) ? -2 : Reader.GetInt32(3)
                    };

                    Cols.Add(Modelo);
                }
                
                if (await HavePrimaryKey(Table))
                {
                    string Key = await GetTablePrimaryKey(Table);

                    var Columna = Cols.Where(x => x.Name == Key).FirstOrDefault();
                    if(Columna != null)
                    {

                        Columna.IsPrimaryKey = true;
                        
                        if (await isColumnIdentity(Table, Key))
                            Columna.Identity = await ColumnIdentityInfo(Table, Key);
                        else
                            Columna.Identity = null;
                    }
                }
            }

            return Cols;
        }
        string ConcatenarColumnas(List<ColumnInfo> Columnas)
        {
            string ColumnasConcatenadas = "";

            foreach (ColumnInfo Columna in Columnas)
            {
                if (ColumnasConcatenadas == "")
                    ColumnasConcatenadas = Columna.Name;
                else
                    ColumnasConcatenadas += ", " + Columna.Name;
            }

            return ColumnasConcatenadas;
        }
        async Task<IdentityInfo> ColumnIdentityInfo(string Table, string Column)
        {
            var Modelo = new IdentityInfo();
            string CmdQuery = string.Format("SELECT * from SYS.IDENTITY_COLUMNS where object_id = OBJECT_ID( '{0}') AND name = '{1}'", Table, Column);
            using (SqlCommand CMD = new SqlCommand(CmdQuery, Connection))
            {
                var Reader = await Execute(CMD);

                await Reader.ReadAsync();

                Modelo.Seed = Reader.GetInt32(22);
                Modelo.Increment = Reader.GetInt32(23);

                return Modelo;
            }
        }
        async Task<string> GetTablePrimaryKey(string Table)
        {
            string CmdQuery = string.Format(GET_TABLE_PRIMARYKEY, Table);
            using (SqlCommand CMD = new SqlCommand(CmdQuery, Connection))
            {
                var Reader = await Execute(CMD);
                await Reader.ReadAsync();

                return Reader.GetString(0);
            }
        }
        #endregion
        #region bool_responses
        async Task<bool> isColumnIdentity(string Table, string Column)
        {
            string CmdQuery = string.Format(IS_IDENTITY_COLUMN, Table, Column);
            return await Exists(CmdQuery);
        }
        async Task<bool> HavePrimaryKey(string Table)
        {
            string CmdQuery = string.Format(GET_TABLE_PRIMARYKEY, Table);
            return await Exists(CmdQuery);
        }
        #endregion
        #region helpers
        void Query( string cmd )
        {
            if (Connection == null)
                throw new NotOpenSqlConnection();

            using (var SqlCmd = new SqlCommand(cmd, Connection))
                SqlCmd.ExecuteNonQuery();
            
        }
        async Task<DataTable> ExecuteQuery(string Cmd)
        {
            var Data = new DataTable();

            using (SqlCommand CMD = new SqlCommand(Cmd, Connection))
            {

                using (var Reader = await Execute(CMD))
                    Data.Load(Reader);

            }

            return Data;
        }
        async Task<bool> Exists(string Query)
        {
            using (SqlCommand CMD = new SqlCommand(Query, Connection))
            {
                var Reader = await Execute(CMD);

                return Reader.HasRows;
            }
        }
        string DropIfExistsString( string Tabla) => string.Format(DROP_TABLE, Tabla);
        public void SelectDb(string Database) => Query(string.Format(SELECT_DATABASE, Database));
        async Task<SqlDataReader> Execute(SqlCommand CMD) => await CMD.ExecuteReaderAsync();
        #endregion
    }

    public class NotOpenSqlConnection : Exception { public NotOpenSqlConnection() : base("No SQL Connection Open.") { } }
}
