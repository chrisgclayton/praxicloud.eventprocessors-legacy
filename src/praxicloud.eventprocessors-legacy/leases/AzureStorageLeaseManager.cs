// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
    /// An event processor host lease manager that is backed by Azure Storage. 
    /// </summary>
    public sealed class AzureStorageLeaseManager : IPraxiLeaseManager
    {
        #region Constants
        /// <summary>
        /// The owner to place in the lease contents
        /// </summary>
        private static string MetaDataOwnerName = "AZBLOBLEASEMGR";
        #endregion
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
        private readonly CloudBlobContainer _leaseStoreContainer;

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
        /// The name of the event processor host
        /// </summary>
        private string _eventProcessorHostName;

        /// <summary>
        /// Metric recorder for the number of reads
        /// </summary>
        private readonly ICounter _leaseReadCounter;

        /// <summary>
        /// Metric recorder for the number of writes
        /// </summary>
        private readonly ICounter _leaseUpdateCounter;

        /// <summary>
        /// Metric recorder for the number of errors encountered
        /// </summary>
        private readonly ICounter _leaseErrorCounter;

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
        /// <param name="metricFactory">The factory to create metric recorders from</param>
        /// <param name="consumerGroupName">The name of the consumer group that the processor is associated with</param>
        /// <param name="connectionString">The connection string used to access the Azure BLOB store</param>
        /// <param name="containerName">The name of the container that the BLOBs are contained in</param>
        /// <param name="subContainerPrefix">The prefix for the BLOB container</param>
        public AzureStorageLeaseManager(ILogger logger, IMetricFactory metricFactory, string consumerGroupName, string connectionString, string containerName, string subContainerPrefix) : this(logger, metricFactory, consumerGroupName, CloudStorageAccount.Parse(connectionString), containerName, subContainerPrefix)
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">The factory to create metric recorders from</param>
        /// <param name="consumerGroupName">The name of the consumer group that the processor is associated with</param>
        /// <param name="storageAccount">The storage account used to access the Azure BLOB store</param>
        /// <param name="containerName">The name of the container that the BLOBs are contained in</param>
        /// <param name="subContainerPrefix">The prefix for the BLOB container</param>
        public AzureStorageLeaseManager(ILogger logger, IMetricFactory metricFactory, string consumerGroupName, CloudStorageAccount storageAccount, string containerName, string subContainerPrefix)
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
                _leaseStoreContainer = storageClient.GetContainerReference(containerName);
                _consumerGroupDirectory = _leaseStoreContainer.GetDirectoryReference($"{subContainerPrefix}{consumerGroupName}");

                _leaseReadCounter = metricFactory.CreateCounter("alm-lease-read", "The number of times that the Azure Storage lease manager has read the lease", false, new string[0]);
                _leaseUpdateCounter = metricFactory.CreateCounter("alm-lease-update", "The number of times that the Azure Storage lease manager has updated the lease", false, new string[0]);
                _leaseErrorCounter = metricFactory.CreateCounter("alm-lease-error", "The number of times that the Azure Storage lease manager has errors raised", false, new string[0]);
                _storagePerformanceSummary = metricFactory.CreateSummary("alm-storage-timing", "The duration taken to access Azure Storage to perform lease manager operations", 10, false, new string[0]);
            }
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public TimeSpan LeaseRenewInterval { get; private set; }

        /// <inheritdoc />
        public TimeSpan LeaseDuration { get; private set; }
        #endregion
        #region Methods
        /// <inheritdoc />
        public Task InitializeAsync(EventProcessorHost host)
        {
            using (_logger.BeginScope("Initializing"))
            {
                _logger.LogInformation("Initializing host {hostName}", host.HostName);

                LeaseDuration = host.PartitionManagerOptions?.LeaseDuration ?? TimeSpan.FromSeconds(20);
                LeaseRenewInterval = host.PartitionManagerOptions?.RenewInterval ?? TimeSpan.FromSeconds(10);

                _defaultRequestOptions = new BlobRequestOptions()
                {
                    ServerTimeout = TimeSpan.FromSeconds(LeaseRenewInterval.TotalSeconds / 2),
                    MaximumExecutionTime = TimeSpan.FromSeconds(LeaseRenewInterval.TotalSeconds)
                };

                _eventProcessorHostName = host.HostName;
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task<bool> CreateLeaseStoreIfNotExistsAsync()
        {
            bool success;

            using (_logger.BeginScope("Create lease store"))
            {
                _logger.LogInformation("Creating Azure Storage lease store");

                using (_storagePerformanceSummary.Time())
                {
                    success = await _leaseStoreContainer.CreateIfNotExistsAsync(_defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    _logger.LogDebug("Lease store creation results {result}", success);
                }
            }

            return success;
        }

        /// <inheritdoc />
        public async Task<bool> DeleteLeaseStoreAsync()
        {
            var errorFound = false;

            BlobContinuationToken storeContinuationToken = null;

            using (_logger.BeginScope("Delete lease store"))
            {
                _logger.LogInformation("Deleting the lease store");

                _leaseUpdateCounter.Increment();

                do
                {
                    BlobResultSegment outerResultSegment;
                    
                    using (_storagePerformanceSummary.Time())
                    {
                        outerResultSegment = await _leaseStoreContainer.ListBlobsSegmentedAsync(storeContinuationToken).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Listing blob segment");
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
                                        _leaseErrorCounter.Increment();
                                        _logger.LogError(e, "Failure while deleting lease store");
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
                                _leaseErrorCounter.Increment();
                                _logger.LogError(e, "Failure while deleting lease store");
                                errorFound = true;
                            }
                        }
                    }
                }
                while (storeContinuationToken != null);

                _logger.LogInformation("Deleted the lease store");
            }

            return !errorFound;
        }

        /// <inheritdoc />
        public async Task<bool> LeaseStoreExistsAsync()
        {
            bool exists;

            using (_logger.BeginScope("Lease Store Exists"))
            {
                _logger.LogInformation("Deleting lease store");

                using (_storagePerformanceSummary.Time())
                {
                    exists = await _leaseStoreContainer.ExistsAsync(_defaultRequestOptions, _operationContext).ConfigureAwait(false);
                }

                _logger.LogDebug("Delete lease store exists: {exists}", exists);
            }

            return exists;
        }

        /// <inheritdoc />
        public async Task<bool> AcquireLeaseAsync(Lease lease)
        {
            var azureLease = (AzureBlobLease)lease;

            CloudBlockBlob leaseBlob = azureLease.Blob;
            bool retval = true;
            string newLeaseId = Guid.NewGuid().ToString();
            string partitionId = lease.PartitionId;

            _leaseUpdateCounter.Increment();

            using (_logger.BeginScope("Acquire Lease"))
            {
                _logger.LogInformation("Acquiring lease for partition {partitionId}", lease.PartitionId);

                try
                {
                    bool renewLease = false;
                    string newToken;

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.FetchAttributesAsync(_defaultAccessCondition, _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    if (leaseBlob.Properties.LeaseState == LeaseState.Leased)
                    {
                        if (string.IsNullOrEmpty(lease.Token))
                        {
                            return false;
                        }

                        _logger.LogInformation("Need to ChangeLease: Partition Id: {partitionId}", lease.PartitionId);
                        renewLease = true;

                        using (_storagePerformanceSummary.Time())
                        {
                            newToken = await leaseBlob.ChangeLeaseAsync(newLeaseId, AccessCondition.GenerateLeaseCondition(lease.Token), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        _logger.LogInformation("Need to AcquireLease: Partition Id: {partitionId}", lease.PartitionId);

                        using (_storagePerformanceSummary.Time())
                        {
                            newToken = await leaseBlob.AcquireLeaseAsync(LeaseDuration, newLeaseId, _defaultAccessCondition, _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                        }
                    }

                    lease.Token = newToken;
                    lease.Owner = _eventProcessorHostName;
                    lease.IncrementEpoch(); // Increment epoch each time lease is acquired or stolen by a new host

                    if (renewLease) await RenewLeaseCoreAsync(azureLease).ConfigureAwait(false);
                    leaseBlob.Metadata[MetaDataOwnerName] = lease.Owner;

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.SetMetadataAsync(AccessCondition.GenerateLeaseCondition(lease.Token), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), _leaseEncoding, AccessCondition.GenerateLeaseCondition(lease.Token), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }
                }
                catch (StorageException e)
                {
                    _leaseErrorCounter.Increment();
                    var se = AzureBlobCommon.CheckForLeaseLostException(partitionId, e, _logger);

                    _logger.LogError(se, "Error acquiring lease for partition {partitionId}", lease.PartitionId);
                    throw se;
                }
            }

            return retval;
        }

        /// <inheritdoc />
        public async Task<Lease> CreateLeaseIfNotExistsAsync(string partitionId)
        {
            AzureBlobLease returnLease;

            using (_logger.BeginScope("Create Lease"))
            {
                _logger.LogInformation("Creating lease for partition {partitionId}", partitionId);

                try
                {
                    CloudBlockBlob leaseBlob = _consumerGroupDirectory.GetBlockBlobReference(partitionId);
                    returnLease = new AzureBlobLease(partitionId, leaseBlob);
                    string jsonLease = JsonConvert.SerializeObject(returnLease);

                    _logger.LogDebug("Creating Lease - Partition: {partition}, consumerGroup: {consumerGroup}", partitionId, _consumerGroupName);

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.UploadTextAsync(jsonLease, _leaseEncoding, _overwriteAccessCondition, _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created Lease - Partition: {partition}, consumerGroup: {consumerGroup}", partitionId, _consumerGroupName);
                }
                catch (StorageException se)
                {
                    if (se.RequestInformation.ErrorCode == BlobErrorCodeStrings.BlobAlreadyExists || se.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMissing)
                    {
                        _logger.LogInformation(se, "Lease already exists, Partition {patitionId}", partitionId);
                        returnLease = (AzureBlobLease)await GetLeaseAsync(partitionId).ConfigureAwait(false);
                    }
                    else
                    {
                        _leaseErrorCounter.Increment();
                        _logger.LogError(se, "CreateLeaseIfNotExist StorageException, Partition {patitionId}", partitionId);
                        throw;
                    }
                }
            }

            return returnLease;
        }

        /// <inheritdoc />
        public async Task DeleteLeaseAsync(Lease lease)
        {
            using (_logger.BeginScope("Delete lease"))
            {
                _logger.LogInformation("Deleting Lease: {partitionId}", lease.PartitionId);

                var azureBlobLease = (AzureBlobLease)lease;

                using (_storagePerformanceSummary.Time())
                {
                    await azureBlobLease.Blob.DeleteIfExistsAsync().ConfigureAwait(false);
                }
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<Lease>> GetAllLeasesAsync()
        {
            using (_logger.BeginScope("Get All Leases"))
            {
                _logger.LogInformation("Getting all leases");

                var leaseList = new List<Lease>();

                BlobContinuationToken continuationToken = null;

                do
                {
                    BlobResultSegment blobsResult;

                    using (_storagePerformanceSummary.Time())
                    {
                        blobsResult = await _consumerGroupDirectory.ListBlobsSegmentedAsync(true, BlobListingDetails.Metadata, 32, continuationToken, _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    foreach (CloudBlockBlob leaseBlob in blobsResult.Results)
                    {
                        // Try getting owner name from existing blob.
                        // This might return null when run on the existing lease after SDK upgrade.
                        leaseBlob.Metadata.TryGetValue(MetaDataOwnerName, out var owner);

                        // Discover partition id from URI path of the blob.
                        var partitionId = leaseBlob.Uri.AbsolutePath.Split('/').Last();
                        leaseList.Add(new AzureBlobLease(partitionId, owner, leaseBlob));
                    }

                    continuationToken = blobsResult.ContinuationToken;

                } while (continuationToken != null);

                return leaseList;
            }
        }

        /// <inheritdoc />
        public async Task<Lease> GetLeaseAsync(string partitionId)
        {
            AzureBlobLease lease;

            using (_logger.BeginScope("Get Lease"))
            {
                _logger.LogInformation("Getting lease for {partitionId}", partitionId);
                lease = await GetLeaseContentsInternalAsync(partitionId).ConfigureAwait(false);
            }

            return lease;
        }

        /// <inheritdoc />
        public async Task<bool> ReleaseLeaseAsync(Lease lease)
        {
            using (_logger.BeginScope("Renew Lease"))
            {
                _logger.LogInformation("Releasing lease: Partition Id: {partitionId}", lease.PartitionId);

                AzureBlobLease azureLease = lease as AzureBlobLease;
                CloudBlockBlob leaseBlob = azureLease.Blob;
                string partitionId = azureLease.PartitionId;

                try
                {
                    string leaseId = lease.Token;
                    AzureBlobLease releasedCopy = new AzureBlobLease(azureLease, leaseBlob)
                    {
                        Token = string.Empty,
                        Owner = string.Empty
                    };

                    leaseBlob.Metadata.Remove(MetaDataOwnerName);
                    _logger.LogDebug("Setting metadata for partition {partitionId}", partitionId);

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.SetMetadataAsync(AccessCondition.GenerateLeaseCondition(leaseId), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Uploading lease data for partition {partitionId}", partitionId);

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(releasedCopy), _leaseEncoding, AccessCondition.GenerateLeaseCondition(leaseId), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Release the lease for for partition {partitionId}", partitionId);

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }
                }
                catch (StorageException e)
                {
                    var se = AzureBlobCommon.CheckForLeaseLostException(partitionId, e, _logger);

                    _logger.LogError(se, "Error releasing lease for partition {partitionId}", lease.PartitionId);
                    throw se;
                }
            }

            return true;
        }

        /// <inheritdoc />
        public Task<bool> RenewLeaseAsync(Lease lease)
        {
            return RenewLeaseCoreAsync((AzureBlobLease)lease);
        }

        /// <inheritdoc />
        public Task<bool> UpdateLeaseAsync(Lease lease)
        {
            return UpdateLeaseCoreAsync((AzureBlobLease)lease);
        }

        /// <summary>
        /// Updates the lease 
        /// </summary>
        /// <param name="lease">The lease to update</param>
        /// <returns>True if updated successfully</returns>
        private async Task<bool> UpdateLeaseCoreAsync(AzureBlobLease lease)
        {
            var success = false;

            if (lease != null && !string.IsNullOrEmpty(lease.Token))
            {
                
            }
            else
            {
                success = true;

                _logger.LogInformation("Updating lease: Partition Id: {partitionId}", lease.PartitionId);

                string partitionId = lease.PartitionId;
                string token = lease.Token;

                CloudBlockBlob leaseBlob = lease.Blob;

                try
                {
                    string jsonToUpload = JsonConvert.SerializeObject(lease);
                    _logger.LogInformation("Updating lease: Partition Id: {partitionId}, RawJson: {json}", lease.PartitionId, jsonToUpload);

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.UploadTextAsync(jsonToUpload, _leaseEncoding, AccessCondition.GenerateLeaseCondition(token), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Updated lease: Partition Id: {partitionId}", lease.PartitionId);
                }
                catch (StorageException e)
                {
                    var se = AzureBlobCommon.CheckForLeaseLostException(partitionId, e, _logger);

                    _logger.LogError(se, "Updating lease for partition {partitionId}", lease.PartitionId);

                    throw se;
                }
            }

            return success;
        }

        /// <summary>
        /// Renews the lease
        /// </summary>
        /// <param name="lease">The lease to renew</param>
        /// <returns>True if the lease is renewed successfully</returns>
        private async Task<bool> RenewLeaseCoreAsync(AzureBlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            string partitionId = lease.PartitionId;

            using (_logger.BeginScope("Renew Lease Core"))
            {
                try
                {
                    _logger.LogDebug("Core renewal for partition {partitionId}", lease.PartitionId);

                    using (_storagePerformanceSummary.Time())
                    {
                        await leaseBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(lease.Token), _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Core renewal completed for partition {partitionId}", lease.PartitionId);
                }
                catch (StorageException e)
                {
                    var se = AzureBlobCommon.CheckForLeaseLostException(partitionId, e, _logger);

                    _logger.LogError(se, "Renewing lease failed for partition {partitionId}", lease.PartitionId);
                    throw se;
                }
            }

            return true;
        }

        /// <summary>
        /// An common method to retrieve the Azure BLOB Lease from BLOB store
        /// </summary>
        /// <param name="partitionId">The partition id to retrieve the checkpoint for</param>
        /// <returns>The checkpoint with contents of the BLOB and the etag</returns>
        private async Task<AzureBlobLease> GetLeaseContentsInternalAsync(string partitionId)
        {
            CloudBlockBlob leaseBlob = _consumerGroupDirectory.GetBlockBlobReference(partitionId);

            using (_logger.BeginScope("Get lease contents internal"))
            {
                _logger.LogDebug("Downloading lease for partition {partitionId}", partitionId);
                string jsonLease = await leaseBlob.DownloadTextAsync(_leaseEncoding, _defaultAccessCondition, _defaultRequestOptions, _operationContext).ConfigureAwait(false);
                _logger.LogDebug("Retrieved lease for partition {id} :: {json}", partitionId, jsonLease);

                var lease = JsonConvert.DeserializeObject<AzureBlobLease>(jsonLease);

                return new AzureBlobLease(lease, leaseBlob);
            }
        }
        #endregion
    }
}