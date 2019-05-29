using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using System.Text;
using System.Collections.Generic;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;

namespace ModulePublish
{
    public class Program : IHostedService
    {
        private static int interval;
        private static bool sendBatch;

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
                    configLogging.AddDebug();
                }).ConfigureServices((hostContext, services) =>
                {
                    services.AddOptions();
                    
                    interval = hostContext.Configuration.GetValue<int>("IntervalMs", 1000);
                    sendBatch = hostContext.Configuration.GetValue<bool>("SendBatch", false);
                    services.AddSingleton<IHostedService, Program>();
                });
            await host.RunConsoleAsync();
        }
        private readonly ConcurrentQueue<DateTime> queue = new ConcurrentQueue<DateTime>();
        private readonly ILogger<Program> logger;

        public Program(ILogger<Program> logger) 
        {
            this.logger = logger;
            
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            //using (var moduleClient = await ModuleClient.CreateFromEnvironmentAsync()) 
            using (var timer = new Timer((_) => queue.Enqueue(DateTime.UtcNow), null, 0, interval))
            {
                //await moduleClient.OpenAsync();

                Func<Task> sendMethod;
                // if (sendBatch) 
                //     sendMethod = () => SendBatchEvents(moduleClient, cancellationToken);
                // else
                //     sendMethod = () => SendSingleEvent(moduleClient, cancellationToken);

                await Task.Factory.StartNew(() => SendBatch(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        private async Task SendSingleEvent(ModuleClient moduleClient, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (queue.TryDequeue(out var result))
                {
                    var lag = DateTime.UtcNow - result;
                    var body = JsonConvert.SerializeObject(new { result, lag });
                    var msg = new Message(Encoding.Unicode.GetBytes(body));
                    await moduleClient.SendEventAsync(msg);
                }
                else
                {
                    await Task.Delay(interval);
                }
            }

        }
        private async Task SendBatchEvents(ModuleClient moduleClient, CancellationToken token)
        {
            var msgs = new List<Message>(100);
            while (!token.IsCancellationRequested)
            {
                if (msgs.Count < 100 && queue.TryDequeue(out var result))
                {
                    var lag = DateTime.UtcNow - result;
                    var body = JsonConvert.SerializeObject(new { result, lag });
                    msgs.Add(new Message(Encoding.Unicode.GetBytes(body)));
                }
                else
                {
                    await Task.Delay(interval);
                }
                await moduleClient.SendEventBatchAsync(msgs);
                msgs.Clear();
            }
        }
        private async Task SendBatch(CancellationToken token)
        {
            var msgs = new List<Message>(10);
            while (!token.IsCancellationRequested)
            {
                if (msgs.Count < 10 && queue.TryDequeue(out var result))
                {
                    var lag = DateTime.UtcNow - result;
                    var body = JsonConvert.SerializeObject(new { result, lag });
                    msgs.Add(new Message(Encoding.Unicode.GetBytes(body)));
                }
                else
                {
                    await Task.Delay(interval);
                }
                if (msgs.Any()) {
                    logger.LogInformation("wrote {0} messages", msgs.Count);
                    msgs.Clear();
                }
            }
        }
    }
}
