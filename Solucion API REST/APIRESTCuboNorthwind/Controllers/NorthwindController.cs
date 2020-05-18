using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;

namespace APIRESTCuboNorthwind.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    [RoutePrefix("v1/Analysis/Northwind")]

    public class NorthwindController : ApiController
    {
        [HttpGet]
        [Route("Testing")]
        public HttpResponseMessage Testing()
        {
            return Request.CreateResponse(HttpStatusCode.OK, "Prueba de API exitosa");
        }

        [HttpGet]
        [Route("Top5/{dim}/{order}")]
        public HttpResponseMessage Top5(string dim, string order="DESC")
        {
            string dimension = string.Empty;

            switch (dim)
            {
                case "Cliente":
                    dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
                    break;
                case "Producto":
                    dimension = "[Dim Producto].[Dim Producto Nombre].CHILDREN";
                    break;
                case "Empleado":
                    dimension = "[Dim Empleado].[Dim Empleado Nombre].CHILDREN";
                    break;
                default:
                    dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
                    break;
            }

            string WITH = @"
                WITH
                SET [TopVentas] AS
                NONEMPTY(
                    ORDER(
                        STRTOSET(@Dimension),
                        [Measures].[Hec Ventas Ventas], "+ order + @"
                    )
                )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    HEAD([TopVentas], 5)
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> clients = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new 
            {
                datosDimension = clients,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", dimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            clients.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                valor = Math.Round(dr.GetDecimal(1))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result  );
        }

        [HttpGet]
        [Route("GetItemByDimension/{dim}")]
        public HttpResponseMessage GetItemByDimension(string dim)
        {
            string dimension = string.Empty;
            string dim2 = string.Empty;

            switch (dim)
            {
                case "Cliente":
                    dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
                    dim2 = "[Dim Cliente].[Dim Cliente Nombre].CURRENTMEMBER.MEMBER_NAME";
                    break;
                case "Producto":
                    dimension = "[Dim Producto].[Dim Producto Nombre].CHILDREN";
                    dim2 = "[Dim Producto].[Dim Producto Nombre].CURRENTMEMBER.MEMBER_NAME";
                    break;
                case "Empleado":
                    dimension = "[Dim Empleado].[Dim Empleado Nombre].CHILDREN";
                    dim2 = "[Dim Empleado].[Dim Empleado Nombre].CURRENTMEMBER.MEMBER_NAME";
                    break;
                case "Tiempo":
                    dimension = "[Dim Tiempo].[Anio].CHILDREN";
                    dim2 = "[Dim Tiempo].[Anio].CURRENTMEMBER.MEMBER_NAME";
                    break;
                default:
                    dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
                    dim2 = "[Dim Cliente].[Dim Cliente Nombre].CURRENTMEMBER.MEMBER_NAME";
                    break;
            }

            string WITH = @"
                WITH
                SET [OrderDImension] AS
                NONEMPTY(
                    ORDER(
                        STRTOSET(@Dimension),
                        " + dim2 + @", ASC
                    )
                )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [OrderDImension]
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> clients = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = clients,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", dimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            clients.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                valor = Math.Round(dr.GetDecimal(1))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }
        

        [HttpPost]
        [Route("GetDataPieByDimension/{dim}")]
        public HttpResponseMessage GetDataPieByDimension(string dim, string[] values)   
        {
            string dimension = string.Empty;
            switch (dim)
            {
                case "Cliente":
                    dimension = "[Dim Cliente].[Dim Cliente Nombre]";
                    break;
                case "Producto":
                    dimension = "[Dim Producto].[Dim Producto Nombre]";
                    break;
                case "Empleado":
                    dimension = "[Dim Empleado].[Dim Empleado Nombre]";
                    break;
                case "Tiempo":
                    dimension = "[Dim Tiempo].[Anio]";
                    break;
                default:
                    dimension = "[Dim Cliente].[Dim Cliente Nombre]";
                    break;
            }


            string WITH = @"
            WITH
                SET [OrderDimension] AS
                NONEMPTY(
                    ORDER(
                    STRTOSET(@Dimension),
                    [Measures].[Hec Ventas Ventas], DESC
                 )
            )
            ";
            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hec Ventas Ventas]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> dimen = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = dimen,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            string valoresDimension = string.Empty;
            foreach (var item in values)
            {
                valoresDimension += "{0}.[" + item + "],";
            }
            valoresDimension =  valoresDimension.TrimEnd(',');
            valoresDimension = string.Format(valoresDimension, dimension);
            valoresDimension = @"{" + valoresDimension + "}";



            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", valoresDimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimen.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                valor = Math.Round(dr.GetDecimal(1))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        [HttpPost]
        [Route("GetDataGeneric/{dim}/{anios}/{meses}")]
        public HttpResponseMessage GetDataGeneric(string dim, string[] anios, string[] meses, string[] values  )
        {
            string dimension = string.Empty;
            switch (dim)
            {
                case "Cliente":
                    dimension = "[Dim Cliente].[Dim Cliente Nombre]";
                    break;
                case "Producto":
                    dimension = "[Dim Producto].[Dim Producto Nombre]";
                    break;
                case "Empleado":
                    dimension = "[Dim Empleado].[Dim Empleado Nombre]";
                    break;
                default:
                    dimension = "[Dim Cliente].[Dim Cliente Nombre]";
                    break;
            }


            string WITH = @"
            WITH
                SET [Info] AS
                NONEMPTY(
                    ORDER(
                    STRTOSET(@Dimension),
                    [Measures].[Hec Ventas Ventas], DESC
                 )
            )
            ";
            string COLUMNS = @"
                {
                    STRTOSET(@Anio)
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                {
                    [Info]
                }*
                {
                    STRTOSET(@Mes)
                }
                ON ROWS
            ";
            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            List<string> dimen = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<string> mes = new List<string>();
            List<string> anio = new List<string>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = dimen,
                datosVenta = ventas,
                datosMes = mes,
                datosAnio = anio,
                datosTabla = lstTabla
            };

            string valoresDimension = string.Empty;
            foreach (var item in values)
            {
                valoresDimension += "{0}.[" + item + "],";
            }
            valoresDimension = valoresDimension.TrimEnd(',');
            valoresDimension = string.Format(valoresDimension, dimension);
            valoresDimension = @"{" + valoresDimension + "}";


            string valoresMes = string.Empty;
            foreach (var item in meses)
            {
                valoresMes += "[Dim Tiempo].[Mes Espaniol].[" + item + "],";
            }
            valoresMes = valoresMes.TrimEnd(',');
            //valoresMes = @"{" + valoresMes + "}";

            string valoresAnio = string.Empty;
            foreach (var item in anios)
            {
                valoresAnio += "[Dim Tiempo].[Anio].[" + item + "],";
            }
            valoresAnio = valoresAnio.TrimEnd(',');
            //valoresAnio = @"{" + valoresAnio + "}";



            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", valoresDimension);
                    cmd.Parameters.Add("Anio", valoresAnio);
                    cmd.Parameters.Add("Mes", valoresMes);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimen.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));
                            mes.Add(dr.GetString(2));
                            anio.Add(dr.GetString(3));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                data = Math.Round(dr.GetDecimal(1)),
                                label = dr.GetString(3)
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }
            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        [HttpPost]
        [Route("GetDataByGenerico/{dim}/{order}")]
        public HttpResponseMessage GetDataByGenerico(string dim, string order, [FromBody] dynamic values)
        {
            string WITH = @"
             WITH 
                 SET [OrderDimension] AS 
                 NONEMPTY(
                     ORDER(
                     STRTOSET(@Dimension),
                     [Measures].[Hec Ventas Ventas], DESC
              )
             )
             ";
            string COLUMNS = @"
                 {
                     [Measures].[Hec Ventas Ventas]
                 }
                 ON COLUMNS,    
             ";
            string ROWS = @"
                 {
                     ([OrderDimension], STRTOSET(@Anio), STRTOSET(@Mes))
                 }
                 ON ROWS
             ";

            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> dimension = new List<string>();
            List<string> anios = new List<string>();
            List<string> meses = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = dimension,
                datosAnios = anios,
                datosMeses = meses,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            string valoresDimension = string.Empty;
            foreach (var item in values.clients)
            {
                valoresDimension += "{0}.[" + item + "],";
            }
            valoresDimension = valoresDimension.TrimEnd(',');
            valoresDimension = string.Format(valoresDimension, dim);
            valoresDimension = @"{" + valoresDimension + "}";

            string valoresAnios = string.Empty;
            foreach (var item in values.years)
            {
                valoresAnios += "[Dim Tiempo].[Anio].[" + item + "],";
            }
            valoresAnios = valoresAnios.TrimEnd(',');
            valoresAnios = @"{" + valoresAnios + "}";

            string valoresMeses = string.Empty;
            foreach (var item in values.months)
            {
                valoresMeses += "[Dim Tiempo].[Mes Espaniol].[" + item + "],";
            }
            valoresMeses = valoresMeses.TrimEnd(',');
            valoresMeses = @"{" + valoresMeses + "}";

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", valoresDimension);
                    cmd.Parameters.Add("Anio", valoresAnios);
                    cmd.Parameters.Add("Mes", valoresMeses);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                            anios.Add(dr.GetString(1));
                            meses.Add(dr.GetString(2));
                            ventas.Add(Math.Round(dr.GetDecimal(3)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                años = dr.GetString(1),
                                meses = dr.GetString(2),
                                valor = Math.Round(dr.GetDecimal(3))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

    }
}
