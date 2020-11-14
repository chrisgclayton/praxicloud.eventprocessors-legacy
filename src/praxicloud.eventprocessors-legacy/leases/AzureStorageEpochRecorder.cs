// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using System;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.eventprocessors.legacy.storage;
    #endregion

    public sealed class AzureStorageEpochRecorder : IEpochRecorder
    {
        #region Variables
        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// The name of the consumer group that the processor is associated with
        /// </summary>
        private readonly string _consumerGroupName;

        /// <summary>
        /// The container provided as the base lease store
        /// </summary>
        private readonly CloudBlobContainer _epochStoreContainer;

        /// <summary>
        /// The directory for the consumer group
        /// </summary>
        private readonly CloudBlobDirectory _consumerGroupDirectory;

        /// <summary>
        /// The default options to reduce the memory of recreation
        /// </summary>
        private BlobRequestOptions _defaultRequestOptions;

        /// <summary>
        /// The default operations context for BLOB storage which will be null
        /// </summary>
        private readonly OperationContext _operationContext = null;

        /// <summary>
        /// The default access conditions to use when accessing BLOB storage
        /// </summary>
        private readonly AccessCondition _defaultAccessCondition = null;

        /// <summary>
        /// Azure BLOB access conditions that overwrite the existing content
        /// </summary>
        private readonly AccessCondition _overwriteAccessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");

        /// <summary>
        /// Set the default encoding type
        /// </summary>
        private readonly Encoding _leaseEncoding = Encoding.ASCII;

        /// <summary>
        /// Metric recorder for the number of reads
        /// </summary>
        private readonly ICounter _epochReadCounter;

        /// <summary>
        /// Metric recorder for the number of writes
        /// </summary>
        private readonly ICounter _epochUpdateCounter;

        /// <summary>
        /// Metric recorder for the number of errors encountered
        /// </summary>
        private readonly ICounter _epochErrorCounter;

        /// <summary>
        /// Tracks the performance of accessing Azure storage, not separated by access type 
        /// </summary>
        private readonly ISummary _storagePerformanceSummary;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="consumerGroupName">The name of the consumer group that the processor is associated with</param>
        /// <param name="connectionString">The connection string used to access the Azure BLOB store</param>
        /// <param name="containerName">The name of the container that the BLOBs are contained in</param>
        /// <param name="subContainerPrefix">The prefix for the BLOB container</param>
        public AzureStorageEpochRecorder(ILogger logger, IMetricFactory metricFactory, string consumerGroupName, string connectionString, string containerName, string subContainerPrefix) : this(logger, metricFactory, consumerGroupName, CloudStorageAccount.Parse(connectionString), containerName, subContainerPrefix)
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="consumerGroupName">The name of the consumer group that the processor is associated with</param>
        /// <param name="storageAccount">The storage account used to access the Azure BLOB store</param>
        /// <param name="containerName">The name of the container that the BLOBs are contained in</param>
        /// <param name="subContainerPrefix">The prefix for the BLOB container</param>
        public AzureStorageEpochRecorder(ILogger logger, IMetricFactory metricFactory, string consumerGroupName, CloudStorageAccount storageAccount, string containerName, string subContainerPrefix)
        {
            Guard.NotNullOrWhitespace(nameof(consumerGroupName), consumerGroupName);
            Guard.NotNull(nameof(storageAccount), storageAccount);
            Guard.NotNull(nameof(logger), logger);
            AzureBlobCommon.ValidContainerName(nameof(containerName), containerName);

            _logger = logger;
            _consumerGroupName = consumerGroupName;

            using (_logger.BeginScope("Azure Storage Lease Manager::ctor"))
            {
                _logger.LogInformation("Creating Azure lease manager for consumer group {consumerGroupName}, container {containerName}", containerName, consumerGroupName);

                subContainerPrefix = (subContainerPrefix != null) ? subContainerPrefix.Trim() : "";

                var storageClient = storageAccount.CreateCloudBlobClient();

                storageClient.DefaultRequestOptions = new BlobRequestOptions { MaximumExecutionTime = TimeSpan.FromSeconds(60) };
                _epochStoreContainer = storageClient.GetContainerReference(containerName);
                _consumerGroupDirectory = _epochStoreContainer.GetDirectoryReference($"{subContainerPrefix}{consumerGroupName}");

                _epochReadCounter = metricFactory.CreateCounter("aer-checkpoint-read", "The number of times that the Azure Storage epoch recorder has read the epoch", false, new string[0]);
                _epochUpdateCounter = metricFactory.CreateCounter("aer-checkpoint-update", "The number of times that the Azure Storage epoch recorder has updated the epoch", false, new string[0]);
                _epochErrorCounter = metricFactory.CreateCounter("aer-checkpoint-error", "The number of times that the Azure Storage epoch recorder has errors raised", false, new string[0]);
                _storagePerformanceSummary = metricFactory.CreateSummary("aer-storage-timing", "The duration taken to access Azure Storage to perform checkpoint recorder operations", 10, false, new string[0]);
            }
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task<EpochOperationResult> AddOrUpdateEpochAsync(EpochValue value, bool force, CancellationToken cancellationToken)
        {
            EpochOperationResult results = EpochOperationResult.Unknown;

            using (_logger.BeginScope("Update epoch"))
            {
                _logger.LogInformation("Adding epoch: {partitionId}, forced: {force}", value.PartitionId, force);

                CloudBlockBlob epochBlob = _consumerGroupDirectory.GetBlockBlobReference(value.PartitionId);

                try
                {
                    _epochUpdateCounter.Increment();
                    AccessCondition accessCondition = force ? AccessCondition.GenerateLeaseCondition("*") : AccessCondition.GenerateLeaseCondition(value.ConcurrencyValue);
                    var content = JsonConvert.SerializeObject(value);

                    _logger.LogDebug("Writing content to partition {partitionId}, raw: {json}", value.PartitionId, content);

                    using (_storagePerformanceSummary.Time())
                    {
                        await epochBlob.UploadTextAsync(content, _leaseEncoding, accessCondition, _defaultRequestOptions, null).ConfigureAwait(false);
                    }

                    value.ConcurrencyValue = epochBlob.Properties.ETag;
                    _logger.LogDebug("Completed write of epoch to partition {partitionId}", value.PartitionId);

                    results = EpochOperationResult.Success;
                }
                catch (StorageException e) when (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    _logger.LogInformation("Epoch content conflict for partition {partitionId}", value.PartitionId);
                    results = EpochOperationResult.Conflict;
                    _epochErrorCounter.Increment();
                }
                catch (Exception e)
                {
                    _epochErrorCounter.Increment();
                    results = EpochOperationResult.Failure;
                    _logger.LogError(e, "Error adding or updating epoch for partition {partitionId}", value.PartitionId);
                }

                return results;
            }
        }

        /// <inheritdoc />
        public async Task<EpochOperationResult> DeleteEpochAsync(EpochValue value, bool force, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("Delete Epoch"))
            {
                _epochUpdateCounter.Increment();

                EpochOperationResult results = EpochOperationResult.Unknown;

                _logger.LogDebug("Deleting epoch for partition {partitionId}", value.PartitionId);

                CloudBlockBlob epochBlob = _consumerGroupDirectory.GetBlockBlobReference(value.PartitionId);

                try
                {
                    AccessCondition accessCondition = force ? AccessCondition.GenerateLeaseCondition("*") : AccessCondition.GenerateLeaseCondition(value.ConcurrencyValue);

                    using (_storagePerformanceSummary.Time())
                    {
                        await epochBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, accessCondition, _defaultRequestOptions, null, cancellationToken);
                    }

                    results = EpochOperationResult.Success;

                    _logger.LogInformation("Deleted epoch for partition {partitionId}", value.PartitionId);
                }
                catch (StorageException e) when (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    _epochErrorCounter.Increment();
                    results = EpochOperationResult.Conflict;
                    _logger.LogError(e, "Conflict deleting epoch for partition {partitionId}", value.PartitionId);
                }
                catch (Exception e)
                {
                    _epochErrorCounter.Increment();
                    results = EpochOperationResult.Failure;
                    _logger.LogError(e, "Error deleting epoch for partition {partitionId}", value.PartitionId);
                }

                return results;
            }
        }

        /// <inheritdoc />
        public async Task<EpochValue> GetEpochAsync(string partitionId, CancellationToken cancellationToken)
        {
            EpochValue epochValue = null;

            using (_logger.BeginScope("Get Epoch"))
            {
                _epochReadCounter.Increment();

                _logger.LogInformation("Getting epoch for partition {partitionId}", partitionId);
                CloudBlockBlob epochBlob = _consumerGroupDirectory.GetBlockBlobReference(partitionId);

                string jsonEpoch;

                using (_storagePerformanceSummary.Time())
                {
                    jsonEpoch = await epochBlob.DownloadTextAsync(_leaseEncoding, _defaultAccessCondition, _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                }

                _logger.LogDebug("Retrieved epoch for partition {id} :: {json}", partitionId, jsonEpoch);

                epochValue = JsonConvert.DeserializeObject<EpochValue>(jsonEpoch);
                epochValue.ConcurrencyValue = epochBlob.Properties.ETag;
            }

            return epochValue;
        }

        /// <inheritdoc />
        public Task<bool> InitializeAsync(string eventProcessorHostName, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("Initialize"))
            {
                _logger.LogInformation("Initializing Azure Storage epoch recorder for host {host}", eventProcessorHostName);

                _defaultRequestOptions = new BlobRequestOptions()
                {
                    ServerTimeout = TimeSpan.FromSeconds(5),
                    MaximumExecutionTime = TimeSpan.FromSeconds(10)
                };
            }

            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public async Task<EpochOperationResult> CreateEpochStoreIfNotExistsAsync(CancellationToken cancellationToken)
        {
            bool success;

            _epochUpdateCounter.Increment();

            using (_logger.BeginScope("Create Epoch Store"))
            {
                _logger.LogInformation("Creating epoch store");

                using (_storagePerformanceSummary.Time())
                {
                    success = await _epochStoreContainer.CreateIfNotExistsAsync(_defaultRequestOptions, _operationContext).ConfigureAwait(false);
                }

                _logger.LogInformation("Created epoch store");
            }

            return success ? EpochOperationResult.Success : EpochOperationResult.Failure;
        }

        /// <inheritdoc />
        public async Task<EpochOperationResult> DeleteEpochStoreAsync(CancellationToken cancellationToken)
        {
            var errorFound = false;
            BlobContinuationToken storeContinuationToken = null;

            _epochUpdateCounter.Increment();

            using (_logger.BeginScope("Delete Epoch Store"))
            {
                _logger.LogInformation("Deleting epoch store");

                do
                {
                    BlobResultSegment outerResultSegment;

                    using (_storagePerformanceSummary.Time())
                    {
                        outerResultSegment = await _epochStoreContainer.ListBlobsSegmentedAsync(storeContinuationToken).ConfigureAwait(false);
                    }

                    storeContinuationToken = outerResultSegment.ContinuationToken;

                    foreach (IListBlobItem blobItem in outerResultSegment.Results)
                    {
                        if (blobItem is CloudBlobDirectory)
                        {
                            BlobContinuationToken containerContinuationToken = null;

                            do
                            {
                                BlobResultSegment containerResultSegment;

                                using (_storagePerformanceSummary.Time())
                                {
                                    containerResultSegment = await ((CloudBlobDirectory)blobItem).ListBlobsSegmentedAsync(containerContinuationToken).ConfigureAwait(false);
                                }

                                containerContinuationToken = containerResultSegment.ContinuationToken;

                                foreach (IListBlobItem subBlob in containerResultSegment.Results)
                                {
                                    try
                                    {
                                        using (_storagePerformanceSummary.Time())
                                        {
                                            await ((CloudBlockBlob)subBlob).DeleteIfExistsAsync().ConfigureAwait(false);
                                        }
                                    }
                                    catch (StorageException e)
                                    {
                                        _epochErrorCounter.Increment();
                                        _logger.LogError(e, "Failure while deleting epoch store");
                                        errorFound = true;
                                    }
                                }
                            }
                            while (containerContinuationToken != null);
                        }
                        else if (blobItem is CloudBlockBlob)
                        {
                            try
                            {
                                using (_storagePerformanceSummary.Time())
                                {
                                    await ((CloudBlockBlob)blobItem).DeleteIfExistsAsync().ConfigureAwait(false);
                                }
                            }
                            catch (StorageException e)
                            {
                                _epochErrorCounter.Increment();
                                _logger.LogError(e, "Failure while deleting epoch store");
                                errorFound = true;
                            }
                        }
                    }
                }
                while (storeContinuationToken != null);
            }

            return !errorFound ? EpochOperationResult.Success : EpochOperationResult.Failure;
        }

        /// <inheritdoc />
        public async Task<bool> EpochStoreExistsAsync(CancellationToken cancellationToken)
        {
            bool exists;

            _epochReadCounter.Increment();

            using (_logger.BeginScope("Epoch Store Exists"))
            {
                _logger.LogInformation("Checking if epoch store exists");

                using (_storagePerformanceSummary.Time())
                {
                    exists = await _epochStoreContainer.ExistsAsync(_defaultRequestOptions, _operationContext).ConfigureAwait(false);
                }

                _logger.LogDebug("Epoch store exists result {exists}", exists);
            }

            return exists;
        }
        #endregion
    }
}
