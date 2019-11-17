using SSqlBackup;
using SSqlBackup.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace testingwp
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        void AppendText( string Texto)
        {
            lblproccess.Text = lblproccess.Text + Texto + "\n";
        }
        private async void button1_Click(object sender, EventArgs e)
        {

            var SSql = new xSqlBackUp();
            SSql.Open(@"Data Source=DESKTOP-B676VD8\SQLEXPRESS;Integrated Security=True;MultipleActiveResultSets=True", "facycred");

            AppendText("INICIANDO...");


            await SSql.BackUp(@"C:\Users\PC\Desktop\resultados.sql");
            AppendText("RESPALDO REALIZADO!");



            //File.WriteAllText(, Respaldo);


        }

        async void PrintCols(xSqlBackUp SSql)
        {
            SSql.SelectDb("facycred");
            var Listado = await SSql.GetColumns("xdd");

            foreach (ColumnInfo Col in Listado)
            {

                string str = string.Format(@"
------------------------------
PRIMARY KEY: {0}
COLUMNA: {1}
NULLABLE: {2}
TIPO DATO: {3}
MAXIMO LARGO: {4}

IDENTIDAD: {5}

------------------------------
", Col.IsPrimaryKey.ToString(), Col.Name, Col.NullAble.ToString(), Col.DataType, Col.MaxLength, Col.Identity != null ? "SI" : "NO");

                AppendText(str);
            }
        }

        private async void button2_Click(object sender, EventArgs e)
        {
            var SSql = new xSqlBackUp();
            SSql.Open(@"Data Source=DESKTOP-B676VD8\SQLEXPRESS;Integrated Security=True;MultipleActiveResultSets=True", "facycred");

            AppendText("INICIANDO...");

            //string SecuenciasSql = File.ReadAllText(@"C:\Facycred_backups\20191116150609_respaldo.sql", Encoding.UTF8);

            //AppendText(SecuenciasSql);


            await SSql.Restore(@"C:\Facycred_backups\20191116150609_respaldo.sql");
            AppendText("CARGA REALIZADA!");
        }
    }
}
