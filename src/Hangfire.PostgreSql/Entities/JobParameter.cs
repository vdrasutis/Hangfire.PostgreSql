using Hangfire.PostgreSql.Properties;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class JobParameter
    {
        public int JobId { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
