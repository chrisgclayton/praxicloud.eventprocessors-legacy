// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;
    using Nito.AsyncEx;
    using praxicloud.core.security;
    using praxicloud.distributed.indexes;
    using praxicloud.distributed.indexes.strings;
    #endregion

    /// <summary>
    /// A partition manager for Event Hub partitions
    /// </summary>
    public sealed class FixedPartitionManager 
    {
        #region Delegates
        /// <summary>
        /// Represents a callback for updates to the number of managers
        /// </summary>
        /// <param name="quantity">The updated number of managers</param>
        public delegate Task ManagerQuantityUpdated(int quantity);
        #endregion
        #region Variables
        /// <summary>
        /// The index manager used to track ownership
        /// </summary>
        private StringIndexManager _indexManager;

        /// <summary>
        /// The zero based index of the manager
        /// </summary>
        private readonly int _managerId;

        /// <summary>
        /// The event hub connection string
        /// </summary>
        private readonly string _connectionString;

        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// A lock to control access to activities that change ownership
        /// </summary>
        private readonly AsyncLock _updateLock = new AsyncLock();
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">A logger to write debugging and diagnostics information to</param>
        /// <param name="connectionString">The Azure Event Hub connection string to retrieve partition details from</param>
        /// <param name="managerId">The zero based manager index</param>
        public FixedPartitionManager(ILogger logger, string connectionString, int managerId)
        {
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNullOrWhitespace(nameof(connectionString), connectionString);
            Guard.NotLessThan(nameof(managerId), managerId, 0);
            Guard.NotMoreThan(nameof(managerId), managerId, 1023);

            _managerId = managerId;
            _connectionString = connectionString;
            _logger = logger;
        }
        #endregion
        #region Properties
        /// <summary>
        /// A handler to call when changes occur to the manager quantity
        /// </summary>
        public ManagerQuantityUpdated QuantityUpdateHandler { get; set; }

        /// <summary>
        /// The number of managers in use
        /// </summary>
        public int ManagerCount => _indexManager.ManagerQuantity;
        #endregion
        #region Methods
        /// <summary>
        /// Initializes the ownership. This must be called before other activities occur
        /// </summary>
        /// <param name="managerQuantity">The new quantity of managers</param>
        public async Task InitializeAsync(int managerQuantity)
        {
            Guard.NotLessThan(nameof(managerQuantity), managerQuantity, 1);
            Guard.NotMoreThan(nameof(managerQuantity), managerQuantity, 1024);

            using (_logger.BeginScope("Initializing Partition Manager"))
            {
                _logger.LogInformation("Initializing fixed partition manager");
                var client = EventHubClient.CreateFromConnectionString(_connectionString);
                client.RetryPolicy = new RetryExponential(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(10), 10);
                var runtimeInformation = await client.GetRuntimeInformationAsync().ConfigureAwait(false);
                var partitions = runtimeInformation.PartitionIds.OrderBy(item => item).ToArray();

                _indexManager = new StringIndexManager(managerQuantity, _managerId, partitions);
                _indexManager.NotificationHandler = ManagerIndexUpdatedAsync;
                await _indexManager.InitializeAsync(CancellationToken.None);
            }
        }

        /// <summary>
        /// Checks to see if the partition specified is owned by the manager
        /// </summary>
        /// <param name="partitionId">The id of the partition to check</param>
        /// <returns>True if the manager owns the specified partition</returns>
        public bool IsOwner(string partitionId)
        {          
            return _indexManager.IsOwner(partitionId);
        }

        /// <summary>
        /// Returns the ids of all partitions owned by the current manager
        /// </summary>
        /// <returns>An array of partition ids that are owned by the manager</returns>
        public string[] GetOwnedPartitions()
        {           
            return _indexManager.OwnedIndexes;
        }

        /// <summary>
        /// Returns the ids of all partitions 
        /// </summary>
        /// <returns>An array of partition ids</returns>
        public string[] GetPartitions()
        {            
            return _indexManager.Indexes;
        }

        /// <summary>
        /// Filters out changes to the state that do not impact the manager quantity
        /// </summary>
        /// <param name="managerQuantity">The new number of managers</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        public async Task UpdateManagerQuantityAsync(int managerQuantity, CancellationToken cancellationToken)
        {
            using(await _updateLock.LockAsync(cancellationToken))
            {
                if(_indexManager.ManagerQuantity != managerQuantity)
                {
                    await _indexManager.UpdateManagerQuantityAsync(managerQuantity, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// An update handler when the index manager notifies of changes
        /// </summary>
        /// <param name="manager">The index manager where the change occurred</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        private Task ManagerIndexUpdatedAsync(IIndexManager<string> manager, CancellationToken cancellationToken)
        {
            return QuantityUpdateHandler != null ? QuantityUpdateHandler(manager.ManagerQuantity) : Task.CompletedTask;
        }
        #endregion
    }
}
