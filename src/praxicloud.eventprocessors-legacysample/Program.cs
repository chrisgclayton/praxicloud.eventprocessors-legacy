// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacysample
{
    #region Using Clauses
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.eventprocessors.legacy.checkpoints;
    using praxicloud.eventprocessors.legacy.leases;
    #endregion
    /// <summary>
    /// The entry point to a sample processor that uses the fixed partition manager for leasing and Azure Storage for checkpointing
    /// </summary>
    class Program
    {
        #region Methods
        /// <summary>
        /// The entry point
        /// </summary>
        static void Main()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// An asynchronous version of the entry point to enable asynchronous all the way down
        /// </summary>
        private static async Task MainAsync()
        {
            var processingManagerId = 0;

            var configuration = GetConfiguration(LogLevel.Warning, LogLevel.Information);
            var metricFactory = GetMetricFactory();

            var processorOptions = new EventProcessorOptions
            {
                InitialOffsetProvider = (partitionId) => EventPosition.FromStart(),
                InvokeProcessorAfterReceiveTimeout = true,
                MaxBatchSize = 100,
                PrefetchCount = 300,
                ReceiveTimeout = TimeSpan.FromSeconds(60)
            };

            var manager = new FixedPartitionManager(configuration.PartitionManagerLogger, configuration.EventHubConnectionString, processingManagerId);

            await manager.InitializeAsync(2).ConfigureAwait(true);
            var epochRecorder = new AzureStorageEpochRecorder(configuration.EpochRecorderLogger, metricFactory, configuration.ConsumerGroupName, configuration.CheckpointConnectionString, configuration.EpochContainerName, null);

            var leaseManager = new FixedLeaseManager(configuration.LeaseLogger, configuration.ConsumerGroupName, manager, epochRecorder);
            var checkpointManager = new AzureStorageCheckpointManager(configuration.CheckpointLogger, metricFactory, configuration.CheckpointConnectionString, configuration.CheckpointContainerName, null);

            var builder = new EventHubsConnectionStringBuilder(configuration.EventHubConnectionString);
            var host = new EventProcessorHost(configuration.EventProcessorHostName, builder.EntityPath, configuration.ConsumerGroupName, builder.ToString(), checkpointManager, leaseManager);

            host.PartitionManagerOptions = new PartitionManagerOptions();

            host.PartitionManagerOptions.RenewInterval = TimeSpan.FromSeconds(5);
            host.PartitionManagerOptions.LeaseDuration = TimeSpan.FromSeconds(15);

            checkpointManager.Initialize(host);
            await leaseManager.InitializeAsync(host).ConfigureAwait(true);
            await host.RegisterEventProcessorAsync<SampleProcessor>(processorOptions).ConfigureAwait(true);

            string line;

            do {
                Console.WriteLine("Event Processor Started. To change the event processor manager count enter a number and press <ENTER> or quit to exit.");
                line = Console.ReadLine();

                if (!string.IsNullOrEmpty(line)) line = line.Trim();

                if (!string.IsNullOrEmpty(line))
                {
                    if (int.TryParse(line, out var managerCount))
                    {
                        if (managerCount < 1 || managerCount > 1025)
                        {
                            Console.WriteLine("Valid numbers are 1 to 1025");
                        }
                        else
                        {
                            await manager.UpdateManagerQuantityAsync(managerCount, CancellationToken.None).ConfigureAwait(true);
                        }
                    }
                    else if (!string.Equals(line, "quit", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine("Did not recognize entry please try again.");
                    }
                }
            } while (!string.Equals(line, "quit", StringComparison.OrdinalIgnoreCase));

            await host.UnregisterEventProcessorAsync().ConfigureAwait(true);

        }

        /// <summary>
        /// Gets a logger at the specified logging level 
        /// </summary>
        /// <param name="name">The name of the logger</param>
        /// <param name="level">The logging level to write debugging and diagnostics information at</param>
        /// <returns>An instance of a logger</returns>
        private static ILogger GetLogger(string name, LogLevel level)
        {
            var collection = new ServiceCollection();

            collection.AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(level);
            });

            var provider = collection.BuildServiceProvider();

            return provider.GetRequiredService<ILoggerFactory>().CreateLogger(name);
        }

        /// <summary>
        /// Retrieves a metric factory to use when outputting metrics
        /// </summary>
        /// <returns>A metrics factory instance that metric recorders can be created from</returns>
        private static IMetricFactory GetMetricFactory()
        {
            var factory = new MetricFactory();

            return factory;
        }

        /// <summary>
        /// Retrieves the configuration for the tests
        /// </summary>
        /// <param name="ephLevel">The level of basic event processor host functions to log information at</param>
        /// <param name="processorLevel">The level that the sample processor should write data at</param>
        /// <returns>An instance of the configuration to use when running the sample</returns>
        private static SampleConfiguration GetConfiguration(LogLevel ephLevel, LogLevel processorLevel)
        {
            return new SampleConfiguration
            {
                CheckpointConnectionString = Environment.GetEnvironmentVariable("iotc_checkpoint_connection"),
                LeaseConnectionString = Environment.GetEnvironmentVariable("iotc_lease_connection"),
                EventHubConnectionString = Environment.GetEnvironmentVariable("iotc_eventhub"),
                CheckpointContainerName = Environment.GetEnvironmentVariable("iotc_checkpoint_container"),
                EventProcessorHostName = Environment.GetEnvironmentVariable("iotc_host"),
                LeaseContainerName = Environment.GetEnvironmentVariable("iotc_lease_container"),
                ConsumerGroupName = Environment.GetEnvironmentVariable("iotc_consumer_group"),
                EpochContainerName = Environment.GetEnvironmentVariable("iotc_epoch_container"),
                CheckpointLogger = GetLogger("checkpoint", ephLevel),
                LeaseLogger = GetLogger("lease", ephLevel),
                ProcessorLogger = GetLogger("processor", processorLevel),
                PartitionManagerLogger = GetLogger("partitions", ephLevel),
                EpochRecorderLogger = GetLogger("epoch", ephLevel)
            };
        }
        #endregion
    }
}
