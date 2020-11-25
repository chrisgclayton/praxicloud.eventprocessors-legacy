# PraxiCloud Event Processors - Legacy
PraxiCloud Libraries are a set of common utilities and tools for general software development that simplify common development efforts for software development. The event processors legacy library contains lease and checkpoint managers to be used with the legacy event processor host.



# Installing via NuGet

Install-Package PraxiCloud-EventProcessors-Legacy



# Checkpoint Management



## Key Types and Interfaces

|Class| Description | Notes |
| ------------- | ------------- | ------------- |
|**AzureStorageCheckpointManager**|A checkpoint manager that stores the watermark information in Azure Storage (Blobs).| This does not leverage a lease to hold ownership of the partition to reduce time to continue after failure. |
|**FileCheckpointManager**|A checkpoint manager that stores the watermark information in a set of files, one for each partition.| This may be used in Kubernetes or other persisted volume storage as they are mounted to the file system. |


## Additional Information

For additional information the Visual Studio generated documentation found [here](./documents/praxicloud.eventprocessors-legacy.xml), can be viewed using your favorite documentation viewer.

# Lease Management



## Key Types and Interfaces

|Class| Description | Notes |
| ------------- | ------------- | ------------- |
|**AzureStorageLeaseManager**|A lease manager that uses Azure BLOB leases to perform leader election.| This maps to the out of the box Lease Manager offered by the event processor host, when in use with the AzureStorageCheckpointManager it is recommended to use the frameworks default mechanisms. |
|**FixedLeaseManager**|A lease manager that uses a fixed index management strategy to perform leader election and ownership of the partitions. This attempts to solve the issue of processor greedy algorithm and time to recovery.| This mechanism is used for systems such as Service Fabric or Kubernetes where an index or known set of replicas is known in advance. |

## Sample Usage

### Use the Fixed Lease Manager with Azure Storage Checkpoint Manager

```csharp
var processorOptions = new EventProcessorOptions
{
    InitialOffsetProvider = (partitionId) => EventPosition.FromStart(),
    InvokeProcessorAfterReceiveTimeout = true,
    MaxBatchSize = 100,
    PrefetchCount = 300,
    ReceiveTimeout = TimeSpan.FromSeconds(15)
};

var manager = new FixedPartitionManager(configuration.PartitionManagerLogger, configuration.EventHubConnectionString, processingManagerId);

await manager.InitializeAsync(2).ConfigureAwait(true);
var epochRecorder = new AzureStorageEpochRecorder(configuration.EpochRecorderLogger, metricFactory, configuration.ConsumerGroupName, configuration.CheckpointConnectionString, configuration.EpochContainerName, null);

var leaseManager = new FixedLeaseManager(configuration.LeaseLogger, manager, epochRecorder);
var checkpointManager = new AzureStorageCheckpointManager(configuration.CheckpointLogger, metricFactory, configuration.CheckpointConnectionString, configuration.CheckpointContainerName, null);

var builder = new EventHubsConnectionStringBuilder(configuration.EventHubConnectionString);

var host = new EventProcessorHost(configuration.EventProcessorHostName, builder.EntityPath, configuration.ConsumerGroupName, builder.ToString(), checkpointManager, leaseManager)
{
    PartitionManagerOptions = new PartitionManagerOptions()
};

host.PartitionManagerOptions.RenewInterval = TimeSpan.FromSeconds(10);
host.PartitionManagerOptions.LeaseDuration = TimeSpan.FromSeconds(20);

checkpointManager.Initialize(host);
await leaseManager.InitializeAsync(host).ConfigureAwait(true);
await host.RegisterEventProcessorAsync<SampleProcessor>(processorOptions).ConfigureAwait(true);

```

## Additional Information

For additional information the Visual Studio generated documentation found [here](./documents/praxicloud.eventprocessors-legacy.xml), can be viewed using your favorite documentation viewer.




