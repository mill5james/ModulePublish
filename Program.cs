using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace ModulePublish
{
    public class Program : IHostedService
    {
        private static int interval;
        private static bool sendBatch;
        private static int batchSize;

        public static async Task Main(string[] args)
        {
            var host = new HostBuilder()
                .ConfigureHostConfiguration(configHost =>
                {
                    configHost.AddJsonFile("appsettings.json", optional: true);
                    configHost.AddEnvironmentVariables();
                })
                .ConfigureLogging((hostContext, configLogging) =>
                {
                    configLogging.AddConfiguration(hostContext.Configuration.GetSection("Logging"));
                    configLogging.AddConsole();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    interval = hostContext.Configuration.GetValue<int>("IntervalMs", 1000);
                    sendBatch = hostContext.Configuration.GetValue<bool>("SendBatch", false);
                    batchSize = hostContext.Configuration.GetValue<int>("BatchSize", 10);

                    services.AddSingleton<IHostedService, Program>();
                });
            await host.RunConsoleAsync();
        }
        private readonly ConcurrentQueue<DateTime> queue = new ConcurrentQueue<DateTime>();
        private readonly ILogger<Program> logger;
        private ModuleClient moduleClient;

        public Program(ILogger<Program> logger)
        {
            this.logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            //moduleClient = await ModuleClient.CreateFromEnvironmentAsync(); 
            //await moduleClient.OpenAsync();

            Func<Task> sendMethod;
            if (sendBatch) 
                sendMethod = () => SendBatchEvents(cancellationToken);
            else
                sendMethod = () => SendSingleEvent(cancellationToken);

            await Task.Factory.StartNew(() => Generator(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            await Task.Factory.StartNew(sendMethod, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            moduleClient?.Dispose();

            return Task.CompletedTask;
        }

        private async Task Generator(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                queue.Enqueue(DateTime.UtcNow);
                await Task.Delay(interval);
            }
        }

        private async Task SendSingleEvent(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (!queue.TryDequeue(out var result)) {
                    await Task.Delay(interval / 2);
                    continue;
                }
                var lag = DateTime.UtcNow - result;
                var body = JsonConvert.SerializeObject(new { result, lag });
                logger.LogInformation("SendSingleEvent: {0} lag", lag);
                var msg = new Message(Encoding.Unicode.GetBytes(body));
                //await moduleClient.SendEventAsync(msg);
            }

        }
        private async Task SendBatchEvents(CancellationToken token)
        {
            var msgs = new List<Message>(batchSize);
            while (!token.IsCancellationRequested)
            {
                while ((msgs.Count < batchSize) && !token.IsCancellationRequested)
                {
                    if (!queue.TryDequeue(out var result)) {
                        await Task.Delay(interval / 2);
                        continue;
                    }
                    var lag = DateTime.UtcNow - result;
                    var body = JsonConvert.SerializeObject(new { result, lag });
                    logger.LogInformation("SendBatchEvents: {0} lag", lag);
                    msgs.Add(new Message(Encoding.Unicode.GetBytes(body)));

                }
                if (msgs.Any())
                {
                    //await moduleClient.SendEventBatchAsync(msgs);
                    logger.LogInformation("wrote {0} messages", msgs.Count);
                    msgs.Clear();
                }
            }
        }
    }
}
