using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;

using System.Web.Http.Cors;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json;
using System.Web.Cors;

namespace APIRESTCuboNorthwind
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            // Web API configuration and services
            var formatters = GlobalConfiguration.Configuration.Formatters;
            var jsonformatter = formatters.JsonFormatter;
            var settings = jsonformatter.SerializerSettings;

            jsonformatter.SerializerSettings.PreserveReferencesHandling = PreserveReferencesHandling.None;
            config.Formatters.Remove(config.Formatters.XmlFormatter);
            settings.Formatting = Formatting.Indented;
            settings.NullValueHandling = NullValueHandling.Ignore;
            settings.ContractResolver = new CamelCasePropertyNamesContractResolver();
            config.EnableCors();

            // Web API routes
            config.MapHttpAttributeRoutes();

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );
        }
    }
}
