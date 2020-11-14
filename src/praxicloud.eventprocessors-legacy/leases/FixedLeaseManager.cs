// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using MathNet.Numerics.LinearAlgebra;
    using Microsoft.Azure.Amqp.Transaction;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Nito.AsyncEx;
    using praxicloud.core.containers;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.distributed.indexes;
    using praxicloud.distributed.indexes.strings;

    #endregion

    /// <summary>
    /// An event processor host lease manager that is backed by a managed index;
    /// </summary>
    public class FixedLeaseManager : IPraxiLeaseManager
    {
        #region Constants
        /// <summary>
        /// The name of the owner to use for other owner partitions
        /// </summary>
        private const string LeaseTokenOther = "NotOwned";

        /// <summary>
        /// A token to use for owned partitions
        /// </summary>
        private const string LeaseToken = "Owned";
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
        /// Set the default encoding type
        /// </summary>
        private readonly Encoding _leaseEncoding = Encoding.ASCII;

        /// <summary>
        /// The name of the event processor host
        /// </summary>
        private string _eventProcessorHostName;

        /// <summary>
        /// An initialized string index manager
        /// </summary>
        private readonly FixedPartitionManager _partitionManager;

        /// <summary>
        /// A control for accessing the store
        /// </summary>
        private readonly AsyncLock _accessControl = new AsyncLock();

        /// <summary>
        /// The kubernetes leases that the store manages
        /// </summary>
        private Dictionary<string, FixedPartitionLease> _leases = new Dictionary<string, FixedPartitionLease>();

        /// <summary>
        /// The store used to record values to
        /// </summary>
        private readonly IEpochRecorder _epochRecorder;

        /// <summary>
        /// The last known epochs
        /// </summary>
        private readonly ConcurrentDictionary<string, (long epoch, string etag)> _epochs = new ConcurrentDictionary<string, (long epoch, string etag)>();
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="consumerGroupName">The name of the consumer group that the processor is associated with</param>
        /// <param name="partitionManager">An initialized string partition manager</param>
        public FixedLeaseManager(ILogger logger, string consumerGroupName, FixedPartitionManager partitionManager) : this(logger, consumerGroupName, partitionManager, new NoopEpochRecorder(logger))
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="consumerGroupName">The name of the consumer group that the processor is associated with</param>
        /// <param name="partitionManager">An initialized string partition manager</param>
        public FixedLeaseManager(ILogger logger, string consumerGroupName, FixedPartitionManager partitionManager, IEpochRecorder epochRecorder)
        {
            Guard.NotNull(nameof(epochRecorder), epochRecorder);
            Guard.NotNullOrWhitespace(nameof(consumerGroupName), consumerGroupName);
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNull(nameof(partitionManager), partitionManager);

            _logger = logger;

            using (_logger.BeginScope("Fixed Lease Manager::ctor"))
            {
                _logger.LogInformation("Creating fixed lease manager for consumer group {consumerGroupName}", consumerGroupName);

                _consumerGroupName = consumerGroupName;
                _partitionManager = partitionManager;
                _epochRecorder = epochRecorder;
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
        public async Task InitializeAsync(EventProcessorHost host)
        {
            using (_logger.BeginScope("Initialize"))
            {
                _logger.LogInformation("Initializing Fixed Lease Manager for host {hostName}", host.HostName);

                LeaseDuration = host.PartitionManagerOptions?.LeaseDuration ?? TimeSpan.FromSeconds(20);
                LeaseRenewInterval = host.PartitionManagerOptions?.RenewInterval ?? TimeSpan.FromSeconds(10);
                _eventProcessorHostName = host.HostName;

                _partitionManager.QuantityUpdateHandler = ManagerQuantityUpdatedAsync;

                await _epochRecorder.InitializeAsync(host.HostName, ContainerLifecycle.CancellationToken).ConfigureAwait(false);
                await _epochRecorder.CreateEpochStoreIfNotExistsAsync(ContainerLifecycle.CancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Handles index manager changes
        /// </summary>
        /// <param name="quantity">The number of managers used to process the index range</param>
        private Task ManagerQuantityUpdatedAsync(int quantity)
        {
            using (_logger.BeginScope("Updating manager quantity"))
            {
                _logger.LogWarning("The Fixed Lease Manager has been notified that the manager quantity has changed to {quantity}.", quantity);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task<bool> CreateLeaseStoreIfNotExistsAsync()
        {
            _logger.LogInformation("Creating lease store");
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public Task<bool> DeleteLeaseStoreAsync()
        {
            _logger.LogInformation("Deleting lease store");
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public Task<bool> LeaseStoreExistsAsync()
        {
            _logger.LogInformation("Checking existance of lease store");
            return Task.FromResult(true);
        }

        private async Task<bool> UpdateEpochAsync(Lease existingLease, long epoch)
        {
            var success = false;
            var partitionId = existingLease.PartitionId;
            var existingEpoch = existingLease.Epoch;
            var concurrencyValue = string.Empty;

            using (_logger.BeginScope("Update epoch"))
            {
                _logger.LogInformation("Update epoch for parttion {partitionId} with epoch {epoch}", partitionId, epoch);

                if (_epochs.TryGetValue(partitionId, out var existingEpochEntry))
                {
                    if (epoch < existingEpochEntry.epoch)
                    {
                        epoch = existingEpochEntry.epoch;
                        concurrencyValue = existingEpochEntry.etag;
                    }
                }

                _logger.LogDebug("Updating epoch information for partition {partitionId}", partitionId);

                _epochs.AddOrUpdate(partitionId, (epoch, concurrencyValue), (partition, updatePair) =>
                {
                    var newEpoch = epoch;
                    var newConcurrencyValue = concurrencyValue;

                    if (updatePair.epoch >= newEpoch)
                    {
                        newEpoch = updatePair.epoch;
                        newConcurrencyValue = updatePair.etag;
                    }

                    return (newEpoch, newConcurrencyValue);
                });

                var finalEpochValue = _epochs[partitionId];

                try
                {
                    _logger.LogDebug("Updating epoch store for partition {partitionId}", partitionId);

                    var result = await _epochRecorder.AddOrUpdateEpochAsync(new EpochValue
                    {
                        ConcurrencyValue = finalEpochValue.etag,
                        Epoch = finalEpochValue.epoch,
                        PartitionId = partitionId,
                        Result = EpochOperationResult.Success
                    }, false, ContainerLifecycle.CancellationToken).ConfigureAwait(false);

                    _logger.LogDebug("Stored updated epoch for Partition {partitionId}", partitionId);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error storing updated epoch");
                }
            }

            return success;
        }

        /// <inheritdoc />
        public async Task<bool> AcquireLeaseAsync(Lease lease)
        {
            Guard.NotNull(nameof(lease), lease);

            var success = false;

            using (_logger.BeginScope("Acquire lease"))
            {
                _logger.LogInformation("Acquiring lease for partition {partitionId}", lease.PartitionId);

                using (await _accessControl.LockAsync().ConfigureAwait(false))
                {
                    var partitionId = lease.PartitionId;
                    var isOwner = _partitionManager.IsOwner(partitionId);

                    if (isOwner)
                    {
                        if (_leases.TryGetValue(partitionId, out var existingLease))
                        {
                            if (!existingLease.IsOwner || !string.IsNullOrWhiteSpace(existingLease.Token))
                            {
                                existingLease.ExpirationTime = DateTime.UtcNow.Add(LeaseDuration);
                                existingLease.IsOwner = true;
                                existingLease.Token = LeaseToken;
                                existingLease.Owner = _eventProcessorHostName;
                                existingLease.IncrementEpoch();

                                await UpdateEpochAsync(existingLease, existingLease.Epoch).ConfigureAwait(false);

                                success = true;
                            }
                        }
                    }
                    else
                    {
                        _logger.LogDebug("Partition {partitionId} not owned", lease.PartitionId);
                    }
                }
            }

            return success;
        }

        /// <inheritdoc />
        public async Task<bool> RenewLeaseAsync(Lease lease)
        {
            Guard.NotNull(nameof(lease), lease);

            var success = false;
            var partitionId = lease.PartitionId;

            using (_logger.BeginScope("Renew lease"))
            {
                _logger.LogInformation("Renewing lease for partition {partitionId}", lease.PartitionId);

                using (await _accessControl.LockAsync().ConfigureAwait(false))
                {
                    var isOwner = _partitionManager.IsOwner(partitionId);

                    _logger.LogDebug("Partition {partitionId} owner check {isOwner}", lease.PartitionId, isOwner);

                    if (_leases.TryGetValue(partitionId, out var existingLease))
                    {
                        if (isOwner == existingLease.IsOwner)
                        {
                            if (isOwner)
                            {
                                existingLease.ExpirationTime = DateTime.UtcNow.Add(LeaseDuration);
                                success = true;
                            }
                        }
                        else
                        {
                            existingLease.IsOwner = isOwner;
                            existingLease.ExpirationTime = DateTime.MinValue;
                            existingLease.Token = isOwner ? string.Empty : LeaseTokenOther;
                        }
                    }
                }
            }

            return success;
        }

        /// <inheritdoc />
        public async Task<Lease> CreateLeaseIfNotExistsAsync(string partitionId)
        {
            Guard.NotNullOrWhitespace(nameof(partitionId), partitionId);

            FixedPartitionLease lease = null;

            using (_logger.BeginScope("Create lease"))
            {
                _logger.LogInformation("Creating lease for partition {partitionId}", partitionId);

                using (await _accessControl.LockAsync().ConfigureAwait(false))
                {
                    var isOwner = _partitionManager.IsOwner(partitionId);

                    _logger.LogDebug("Partition {partitionId}, owner check {isOwner}", partitionId, isOwner);

                    if (_leases.TryGetValue(partitionId, out var existingLease))
                    {
                        _logger.LogDebug("Partition {partitionId} lease exists.", partitionId);

                        if (existingLease.IsOwner != isOwner)
                        {
                            existingLease.IsOwner = isOwner;
                            existingLease.ExpirationTime = isOwner ? DateTime.MinValue : DateTime.MaxValue;
                            existingLease.Token = isOwner ? LeaseToken : LeaseTokenOther;
                        }

                        lease = existingLease.Clone();
                    }
                    else
                    {
                        _logger.LogDebug("Partition {partitionId} lease does not exist.", partitionId);

                        var newLease = new FixedPartitionLease(partitionId)
                        {
                            Epoch = 1,
                            ExpirationTime = DateTime.MinValue,
                            IsOwner = isOwner,
                            Offset = null,
                            Owner = _eventProcessorHostName,
                            SequenceNumber = -1,
                            Token = isOwner ? LeaseToken : LeaseTokenOther,
                        };

                        var result = await _epochRecorder.AddOrUpdateEpochAsync(new EpochValue
                        {
                            ConcurrencyValue = null,
                            Epoch = newLease.Epoch,
                            PartitionId = partitionId,
                            Result = EpochOperationResult.Success
                        }, false, ContainerLifecycle.CancellationToken).ConfigureAwait(false);

                        _leases.Add(partitionId, newLease);
                        lease = newLease.Clone();

                    }
                }
            }

            return lease;
        }

        /// <inheritdoc />
        public async Task DeleteLeaseAsync(Lease lease)
        {
            Guard.NotNull(nameof(lease), lease);

            _logger.LogInformation("Deleting lease for partition {partitionId}", lease.PartitionId);

            using (await _accessControl.LockAsync().ConfigureAwait(false))
            {
                var partitionId = lease.PartitionId;

                if(_leases.ContainsKey(partitionId)) _leases.Remove(partitionId);
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<Lease>> GetAllLeasesAsync()
        {
            List<Lease> leases;

            _logger.LogDebug("Getting all leases");

            using (await _accessControl.LockAsync().ConfigureAwait(false))
            {
                var partitionList = _partitionManager.GetPartitions();
                leases = new List<Lease>(partitionList.Length);

                foreach (var partitionId in partitionList)
                {
                    leases.Add(await GetLeaseInternalAsync(partitionId).ConfigureAwait(false));
                }
            }

            _logger.LogInformation("Retrieved all leases: {leaseCount}", leases.Count);

            return leases;
        }

        /// <inheritdoc />
        public async Task<Lease> GetLeaseAsync(string partitionId)
        {
            Guard.NotNullOrWhitespace(nameof(partitionId), partitionId);
            Lease lease = null;

            _logger.LogInformation("Getting lease for partition {partitionId}", partitionId);

            using(await _accessControl.LockAsync().ConfigureAwait(false))
            {
                lease = await GetLeaseInternalAsync(partitionId).ConfigureAwait(false);
            }

            return lease;
        }

        /// <inheritdoc />
        public async Task<bool> ReleaseLeaseAsync(Lease lease)
        {
            Guard.NotNull(nameof(lease), lease);

            _logger.LogInformation("Releasing lease for partition {partitionId}", lease.PartitionId);

            using (await _accessControl.LockAsync().ConfigureAwait(false))
            {
                var partitionId = lease.PartitionId;
                var isOwner = _partitionManager.IsOwner(partitionId);

                if(_leases.TryGetValue(partitionId, out var existingLease))
                {
                    if(isOwner || existingLease.IsOwner)
                    {
                        existingLease.IsOwner = isOwner;
                        existingLease.ExpirationTime = DateTime.MinValue;
                        existingLease.Owner = string.Empty;
                        existingLease.Token = string.Empty;
                    }
                }
            }

            return true;
        }

        /// <inheritdoc />
        public async Task<bool> UpdateLeaseAsync(Lease lease)
        {
            var success = false;

            using (_logger.BeginScope("Update lease"))
            {
                _logger.LogInformation("Updating lease for partition {partitionId}", lease.PartitionId);

                using (await _accessControl.LockAsync().ConfigureAwait(false))
                {
                    if (lease != null && !string.IsNullOrWhiteSpace(lease.Token))
                    {
                        var partitionId = lease.PartitionId;
                        var isOwner = _partitionManager.IsOwner(partitionId);

                        _logger.LogDebug("Is owner check for partition {partitionId}, {isOwner}", lease.PartitionId, isOwner);

                        if (_leases.TryGetValue(partitionId, out var existingLease))
                        {
                            existingLease.IsOwner = isOwner;

                            if (existingLease.IsOwner)
                            {
                                existingLease.Token = lease.Token;
                                existingLease.Owner = lease.Owner;
                                existingLease.Epoch = lease.Epoch;
                                existingLease.SequenceNumber = lease.SequenceNumber;
                                existingLease.Offset = lease.Offset;

                                success = true;
                            }
                            else
                            {
                                existingLease.Token = LeaseTokenOther;
                                existingLease.ExpirationTime = DateTime.MaxValue;
                            }
                        }
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Retrieves the lease without locking
        /// </summary>
        /// <param name="partitionId">The partition id to get the lease for</param>
        /// <returns>The lease or null</returns>
        private async Task<Lease> GetLeaseInternalAsync(string partitionId)
        {
            Lease lease = null;
            string concurrency = null;

            using (_logger.BeginScope("Get Lease Internal"))
            {
                if (_leases.TryGetValue(partitionId, out var existingLease))
                {
                    lease = existingLease.Clone();

                    var existingEpoch = await _epochRecorder.GetEpochAsync(partitionId, ContainerLifecycle.CancellationToken).ConfigureAwait(false);

                    if (existingEpoch.Success)
                    {
                        _logger?.LogDebug("Retrieved epoch successfully for partition {partitionId}", partitionId);

                        lease.Epoch = existingEpoch.Epoch;
                        concurrency = existingEpoch.ConcurrencyValue;
                    }
                    else
                    {
                        _logger?.LogDebug("No epoch found for partition {partitionId}", partitionId);
                    }

                    _epochs.AddOrUpdate(partitionId, (lease.Epoch, concurrency), (partition, updatePair) => (lease.Epoch >= updatePair.epoch) ? (lease.Epoch, concurrency) : (updatePair.epoch, updatePair.etag));
                    _logger?.LogDebug("Lease found for partition {partitionId}", partitionId);

                }
                else
                {
                    _logger?.LogDebug("Lease not found for partition {partitionId}", partitionId);
                }
            }

            return lease;
        }
        #endregion
    }
}
