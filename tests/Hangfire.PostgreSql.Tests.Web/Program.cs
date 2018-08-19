using Microsoft.AspNetCore.Hosting;
using static Microsoft.AspNetCore.WebHost;

namespace Hangfire.PostgreSql.Tests.Web
{
    public class Program
    {
        public static void Main(string[] args) => 
            CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .Build()
                .Run();
    }
}
