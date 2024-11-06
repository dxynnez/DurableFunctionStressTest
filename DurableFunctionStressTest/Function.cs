namespace Microsoft.CloudManagedDesktop.Services.Function.Auth.Function
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Functions.Worker;
    using Microsoft.DurableTask;
    using Microsoft.DurableTask.Client;
    using Microsoft.Extensions.Configuration;

    public class Functions
    {
        private IConfiguration configuration;

        public Functions(IConfiguration configuration)
        {
            this.configuration = configuration;
        }

        [Function(nameof(TimerTrigger))]
        public async Task TimerTrigger(
            [TimerTrigger("0 */10 * * * *")] TimerInfo timeInfo,
            [DurableClient(TaskHub = "%TaskHubName%")] DurableTaskClient starter)
        {
            Variables variables = this.configuration.GetSection(Variables.SectionName).Get<Variables>() ?? new Variables();
            for (int i = 0; i < variables.OrchestratorDop; i++)
            {
                await starter.ScheduleNewOrchestrationInstanceAsync(nameof(this.Orchestrator), variables);
            }
        }

        [Function(nameof(Orchestrator))]
        public async Task Orchestrator([OrchestrationTrigger] TaskOrchestrationContext context, Variables variables)
        {
            int subOrchDop = await context.CallActivityAsync<int>(nameof(this.RetrieveSubOrchDop), variables);

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < subOrchDop; i++)
            {
                tasks.Add(context.CallSubOrchestratorAsync(nameof(this.SubOrchestrator), variables));
            }

            await Task.WhenAll(tasks);
        }

        [Function(nameof(SubOrchestrator))]
        public async Task SubOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context, Variables variables)
        {
            int activityDop = await context.CallActivityAsync<int>(nameof(this.RetrieveActivityDop), variables);

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < activityDop; i++)
            {
                tasks.Add(context.CallActivityAsync(nameof(this.ActualWork), variables));
            }

            await Task.WhenAll(tasks);
        }

        [Function(nameof(RetrieveSubOrchDop))]
        public async Task<int> RetrieveSubOrchDop([ActivityTrigger] Variables variables, FunctionContext executionContext)
        {
            await Task.Delay(100);

            return variables.SubOrchestratorDop;
        }

        [Function(nameof(RetrieveActivityDop))]
        public async Task<int> RetrieveActivityDop([ActivityTrigger] Variables variables, FunctionContext executionContext)
        {
            await Task.Delay(100);

            return variables.ActivityDop;
        }

        [Function(nameof(ActualWork))]
        public async Task ActualWork([ActivityTrigger] Variables variables, FunctionContext executionContext)
        {
            await Task.Delay(100);
        }

        public class Variables
        {
            public const string SectionName = nameof(Variables);

            public int OrchestratorDop { get; set; } = 800;

            public int SubOrchestratorDop { get; set; } = 100;

            public int ActivityDop { get; set; } = 2;
        }
    }
}