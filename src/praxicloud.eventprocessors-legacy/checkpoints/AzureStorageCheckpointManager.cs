// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.checkpoints
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.eventprocessors.legacy.storage;
    #endregion

    /// <summary>
    /// An event processor host checkpoint manager that is backed by Azure Storage. The manager does not leverage leases to manage the checkpoint blobs, leaving it for the lease manager to ensure it is writing when valid
    /// </summary>
    public class AzureStorageCheckpointManager : IPraxiCheckpointManager
    {
        #region Variables
        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// The container provided as the base checkpoint store
        /// </summary>
        private CloudBlobContainer _checkpointStoreContainer;

        /// <summary>
        /// The directory for the consumer group
        /// </summary>
        private CloudBlobDirectory _consumerGroupDirectory;

        /// <summary>
        /// The default options to reduce the memory of recreation
        /// </summary>
        private BlobRequestOptions _defaultRequestOptions;

        /// <summary>
        /// Set the default encoding type
        /// </summary>
        private readonly Encoding _checkpointEncoding = Encoding.ASCII;

        /// <summary>
        /// Azure BLOB Storage client
        /// </summary>
        private readonly CloudBlobClient _client;

        /// <summary>
        /// The container that the checkpoints are stored in
        /// </summary>
        private readonly string _containerName;

        /// <summary>
        /// Prefix for the blob storage container
        /// </summary>
        private readonly string _subContainerPrefix;

        /// <summary>
        /// Tracks the last etag values for checkpoints to perform optimistic locking
        /// </summary>
        private readonly ConcurrentDictionary<string, string> _etags = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// Metric recorder for the number of reads
        /// </summary>
        private readonly ICounter _checkpointReadCounter;

        /// <summary>
        /// Metric recorder for the number of writes
        /// </summary>
        private readonly ICounter _checkpointUpdateCounter;

        /// <summary>
        /// Metric recorder for the number of errors encountered
        /// </summary>
        private readonly ICounter _checkpointErrorCounter;

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
        /// <param name="connectionString">The connection string used to access the Azure BLOB store</param>
        /// <param name="containerName">The name of the container that the BLOBs are contained in</param>
        /// <param name="subContainerPrefix">The prefix for the BLOB container</param>
        public AzureStorageCheckpointManager(ILogger logger, IMetricFactory metricFactory, string connectionString, string containerName, string subContainerPrefix) : this(logger, metricFactory, CloudStorageAccount.Parse(connectionString), containerName, subContainerPrefix)
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="storageAccount">The storage account used to access the Azure BLOB store</param>
        /// <param name="containerName">The name of the container that the BLOBs are contained in</param>
        /// <param name="subContainerPrefix">The prefix for the BLOB container</param>
        public AzureStorageCheckpointManager(ILogger logger, IMetricFactory metricFactory, CloudStorageAccount storageAccount, string containerName, string subContainerPrefix)
        {
            Guard.NotNull(nameof(storageAccount), storageAccount);
            Guard.NotNull(nameof(logger), logger);
            AzureBlobCommon.ValidContainerName(nameof(containerName), containerName);

            _logger = logger;

            using (_logger.BeginScope("Azure Storage Checkpoint Manager::ctor"))
            {
                _logger.LogInformation("Creating Azure storage checkpoint manager for container {containerName}", containerName);
                _containerName = containerName;
                _subContainerPrefix = (subContainerPrefix != null) ? subContainerPrefix.Trim() : "";

                _client = storageAccount.CreateCloudBlobClient();

                _checkpointReadCounter = metricFactory.CreateCounter("acm-checkpoint-read", "The number of times that the Azure Storage checkpoint manager has read the checkpoint", false, new string[0]);
                _checkpointUpdateCounter = metricFactory.CreateCounter("acm-checkpoint-update", "The number of times that the Azure Storage checkpoint manager has updated the checkpoint", false, new string[0]);
                _checkpointErrorCounter = metricFactory.CreateCounter("acm-checkpoint-error", "The number of times that the Azure Storage checkpoint manager has errors raised", false, new string[0]);
                _storagePerformanceSummary = metricFactory.CreateSummary("acm-storage-timing", "The duration taken to access Azure Storage to perform checkpoint manager operations", 10, false, new string[0]);
            }
        }
        #endregion
        #region Methods
        /// <summary>
        /// Initializes the lease manager for use, requiring access to the Event Processor Host details
        /// </summary>
        /// <param name="host">The Event Processor Host the lease manager is associated with</param>
        public void Initialize(EventProcessorHost host)
        {
            using (_logger.BeginScope("Initializing"))
            {
                _logger.LogDebug("Initializing");
                var timeoutSeconds = host.PartitionManagerOptions?.LeaseDuration.TotalSeconds ?? 20;

                // Create storage default request options.
                _defaultRequestOptions = new BlobRequestOptions()
                {
                    // Gets or sets the server timeout interval for a single HTTP request.
                    ServerTimeout = TimeSpan.FromSeconds(timeoutSeconds / 2),

                    // Gets or sets the maximum execution time across all potential retries for the request.
                    MaximumExecutionTime = TimeSpan.FromSeconds(timeoutSeconds)
                };

                _client.DefaultRequestOptions = new BlobRequestOptions { MaximumExecutionTime = TimeSpan.FromSeconds(timeoutSeconds / 2) };
                _checkpointStoreContainer = _client.GetContainerReference(_containerName);
                _consumerGroupDirectory = _checkpointStoreContainer.GetDirectoryReference($"{_subContainerPrefix}{host.ConsumerGroupName}");

                _logger.LogInformation("Initialized checkpoint manager. Consumer Group: {consumerGroup}, Host Name: {hostName}, Container Name: {containerName}", host.ConsumerGroupName, host.HostName, _containerName);
            }
        }

        /// <inheritdoc />
        public async Task<bool> CheckpointStoreExistsAsync()
        {
            var success = false;

            using (_logger.BeginScope("Checkpoint Store Exists"))
            {
                _logger.LogDebug("Check if store exists");

                using (_storagePerformanceSummary.Time())
                {
                    success = await _checkpointStoreContainer.ExistsAsync(_defaultRequestOptions, AzureBlobCommon.DefaultOperationContext).ConfigureAwait(false);
                }

                _logger.LogDebug("Check if store exists result {result}", success);
            }

            return success;
        }

        /// <inheritdoc />
        public async Task<bool> CreateCheckpointStoreIfNotExistsAsync()
        {
            var success = false;

            using (_logger.BeginScope("Create store"))
            {
                _logger.LogDebug("Create store if not exists");

                using (_storagePerformanceSummary.Time())
                {
                    success = await _checkpointStoreContainer.CreateIfNotExistsAsync(_defaultRequestOptions, AzureBlobCommon.DefaultOperationContext).ConfigureAwait(false);
                }

                _logger.LogDebug("Create store if not exists result {result}", success);
            }

            return success;
        }

        /// <inheritdoc />
        public async Task DeleteCheckpointAsync(string partitionId)
        {
            using (_logger.BeginScope("Delete Store"))
            {
                _logger.LogInformation("Deleting checkpoint store");
                _checkpointUpdateCounter.Increment();

                BlobContinuationToken storeContinuationToken = null;

                do
                {
                    _logger.LogDebug("Listing blobs in segment");

                    BlobResultSegment outerResultSegment;

                    using (_storagePerformanceSummary.Time())
                    {
                        outerResultSegment = await _checkpointStoreContainer.ListBlobsSegmentedAsync(storeContinuationToken).ConfigureAwait(false);
                    }

                    storeContinuationToken = outerResultSegment.ContinuationToken;

                    _logger.LogDebug("Iterating over blobs");

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
                                        _checkpointErrorCounter.Increment();
                                        _logger.LogError(e, "Failure while deleting checkpoint store");
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
                                _checkpointErrorCounter.Increment();
                                _logger.LogError(e, "Failure while deleting checkpoint store");
                            }
                        }
                    }
                }
                while (storeContinuationToken != null);

                _logger.LogInformation("Deleted checkpoint store");
            }
        }

        /// <inheritdoc />
        public async Task<Checkpoint> GetCheckpointAsync(string partitionId)
        {
            using (_logger.BeginScope("Getting Checkpoint"))
            {
                _logger.LogDebug("Getting checkpoint for partition {partitionId}", partitionId);

                (CheckpointLease lease, string etag) leaseInformation;

                using (_storagePerformanceSummary.Time())
                {
                    leaseInformation = await GetCheckpointInternalAsync(partitionId).ConfigureAwait(false);
                }

                _logger.LogDebug("Retrieved checkpoint for partition {partitionId}", partitionId);
                _etags.AddOrUpdate(partitionId, leaseInformation.etag, (pid, petag) => leaseInformation.etag);
                var lease = leaseInformation.lease;

                return (lease != null && !string.IsNullOrEmpty(lease.Offset)) ? new Checkpoint(partitionId)
                {
                    Offset = lease.Offset,
                    SequenceNumber = lease.SequenceNumber
                } : null;
            }
        }

        /// <inheritdoc />
        public async Task UpdateCheckpointAsync(Lease lease, Checkpoint checkpoint)
        {
            using (_logger.BeginScope("Updating checkpoint"))
            {
                _checkpointUpdateCounter.Increment();

                CheckpointLease blobLease = new CheckpointLease(lease)
                {
                    Offset = checkpoint.Offset,
                    SequenceNumber = checkpoint.SequenceNumber,
                    PartitionId = checkpoint.PartitionId
                };

                _logger.LogInformation("Updating checkpoint: Partition Id: {partitionId}", blobLease.PartitionId);

                CloudBlockBlob leaseBlob = _consumerGroupDirectory.GetBlockBlobReference(blobLease.PartitionId);

                try
                {
                    string jsonToUpload = JsonConvert.SerializeObject(lease);

                    _logger.LogTrace("Updating checkpoint: Partition Id: {partitionId}, RawJson: {json}", blobLease.PartitionId, jsonToUpload);

                    AccessCondition accessCondition;

                    if (_etags.ContainsKey(lease.PartitionId))
                    {
                        accessCondition = AccessCondition.GenerateIfMatchCondition(_etags[lease.PartitionId]);
                    }
                    else
                    {
                        accessCondition = AzureBlobCommon.DefaultAccessCondition;
                    }

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.UploadTextAsync(jsonToUpload, _checkpointEncoding, accessCondition, _defaultRequestOptions, AzureBlobCommon.DefaultOperationContext).ConfigureAwait(false);
                    }

                    _etags.AddOrUpdate(lease.PartitionId, leaseBlob.Properties.ETag, (pid, petag) => leaseBlob.Properties.ETag);

                    _logger.LogInformation("Updated checkpoint for partition {partitionId}", blobLease.PartitionId);
                }
                catch (StorageException e)
                {
                    _checkpointErrorCounter.Increment();
                    _logger.LogError(e, "Error updating partition {partitionId}", blobLease.PartitionId);

                    throw AzureBlobCommon.HandleStorageException(blobLease.PartitionId, e, _logger);
                }
            }
        }

        /// <inheritdoc />
        public async Task<Checkpoint> CreateCheckpointIfNotExistsAsync(string partitionId)
        {
            using (_logger.BeginScope("Create checkpoint"))
            {
                _checkpointUpdateCounter.Increment();

                CheckpointLease checkpoint;

                _logger.LogInformation("Creating checkpoint for partition {partitionId}", partitionId);

                try
                {
                    checkpoint = new CheckpointLease(partitionId);
                    string json = JsonConvert.SerializeObject(checkpoint);

                    CloudBlockBlob checkpointBlob = _consumerGroupDirectory.GetBlockBlobReference(partitionId);

                    bool checkpointExists;

                    using (_storagePerformanceSummary.Time())
                    {
                        checkpointExists = await checkpointBlob.ExistsAsync().ConfigureAwait(false);
                    }

                    if (!checkpointExists)
                    {
                        _logger.LogDebug("Checkpoint does not exist for partition {partitionId}", partitionId);

                        using (_storagePerformanceSummary.Time())
                        {
                            await checkpointBlob.UploadTextAsync(json, _checkpointEncoding, AzureBlobCommon.OverwriteAccessCondition, _defaultRequestOptions, AzureBlobCommon.DefaultOperationContext).ConfigureAwait(false);
                        }

                        _logger.LogDebug("Uploaded checkpoint information for partition {partitionId}", partitionId);
                    }
                    else
                    {
                        _logger.LogDebug("Checkpoint exists, retrieving checkpoint for partition {partitionId}", partitionId);
                        var checkpointInfo = await GetCheckpointInternalAsync(partitionId).ConfigureAwait(false);
                        _logger.LogDebug("Checkpoint retrieved for partition {partitionId}", partitionId);

                        checkpoint = checkpointInfo.lease;
                        _etags.AddOrUpdate(partitionId, checkpointInfo.etag, (pid, petag) => checkpointInfo.etag);
                    }
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.ErrorCode == BlobErrorCodeStrings.BlobAlreadyExists || e.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMissing)
                    {
                        // The blob already exists.
                        _logger.LogInformation("Checkpoint already exists, Partition {patitionId}", partitionId);

                        var checkpointInfo = await GetCheckpointInternalAsync(partitionId).ConfigureAwait(false);
                        checkpoint = checkpointInfo.lease;
                        _etags.AddOrUpdate(partitionId, checkpointInfo.etag, (pid, petag) => checkpointInfo.etag);
                    }
                    else
                    {
                        _checkpointErrorCounter.Increment();
                        _logger.LogError(e, "CreateCheckpointIfNotExist StorageException, Partition {patitionId}", partitionId);
                        throw;
                    }
                }

                return checkpoint;
            }
        }

        /// <summary>
        /// An common method to retrieve the Azure BLOB Lease from BLOB store
        /// </summary>
        /// <param name="partitionId">The partition id to retrieve the checkpoint for</param>
        /// <returns>The checkpoint with contents of the BLOB and the etag</returns>
        private async Task<(CheckpointLease lease, string etag)> GetCheckpointInternalAsync(string partitionId)
        {
            using (_logger.BeginScope("Get checkpoint internal"))
            {
                CloudBlockBlob leaseBlob = _consumerGroupDirectory.GetBlockBlobReference(partitionId);

                string jsonCheckpoint;

                using (_storagePerformanceSummary.Time())
                {
                    jsonCheckpoint = await leaseBlob.DownloadTextAsync(_checkpointEncoding, AzureBlobCommon.DefaultAccessCondition, _defaultRequestOptions, AzureBlobCommon.DefaultOperationContext).ConfigureAwait(false);
                }

                _logger.LogDebug("Retrieved checkpoint for partition {id} :: {json}", partitionId, jsonCheckpoint);
                CheckpointLease lease = JsonConvert.DeserializeObject<CheckpointLease>(jsonCheckpoint);

                _checkpointReadCounter.Increment();

                return (new CheckpointLease(lease), leaseBlob.Properties.ETag);
            }
        }
        #endregion
    }
}
