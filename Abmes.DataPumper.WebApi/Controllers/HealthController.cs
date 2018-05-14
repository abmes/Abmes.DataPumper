using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace Abmes.DataPumper.WebApi.Controllers
{
    [Route("[controller]")]
    public class HealthController : Controller
    {
        public HealthController()
        {
        }

        [HttpGet]
        public string Get()
        {
            return "OK";
        }
    }
}
