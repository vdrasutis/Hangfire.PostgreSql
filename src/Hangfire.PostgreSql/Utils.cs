using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.PostgreSql
{
    internal static class Utils
    {
        public delegate TDto JobSelector<TDto>(SqlJob sqlJob, Job job, Dictionary<string, string> state);
        public static JobList<TDto> DeserializeJobs<TDto>(ICollection<SqlJob> jobs, JobSelector<TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var stateData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData);
                var dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);

                result.Add(new KeyValuePair<string, TDto>(job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        public static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }
    }
}
