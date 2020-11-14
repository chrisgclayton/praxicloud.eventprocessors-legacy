// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using Microsoft.Extensions.Logging;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// An epoch recorder that does not store the epoch
    /// </summary>
    public sealed class NoopEpochRecorder : IEpochRecorder
    {
        #region Variables
        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private readonly ILogger _logger;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new intance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        public NoopEpochRecorder(ILogger logger)
        {
            _logger = logger;
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        public Task<EpochOperationResult> AddOrUpdateEpochAsync(EpochValue value, bool force, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Updated epoch for partition {partitionId}", value.PartitionId);
            return Task.FromResult(EpochOperationResult.Success);
        }

        /// <inheritdoc />
        public Task<EpochOperationResult> DeleteEpochAsync(EpochValue value, bool force, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Deleting epoch for partition {partitionId}", value.PartitionId);
            return Task.FromResult(EpochOperationResult.Success);
        }

        /// <inheritdoc />
        public Task<EpochValue> GetEpochAsync(string partitionId, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Getting epoch for partition {partitionId}", partitionId);

            return Task.FromResult(new EpochValue
            {
                ConcurrencyValue = "*",
                Epoch = 1,
                PartitionId = partitionId,
                Result = EpochOperationResult.Success
            });
        }

        /// <inheritdoc />
        public Task<bool> InitializeAsync(string eventProcessorHostName, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Initializing epoch recorder for {hostName}", eventProcessorHostName);
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public Task<EpochOperationResult> DeleteEpochStoreAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Delete epoch store");
            return Task.FromResult(EpochOperationResult.Success);
        }

        /// <inheritdoc />
        public Task<bool> EpochStoreExistsAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Check if epoch store exists");
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public Task<EpochOperationResult> CreateEpochStoreIfNotExistsAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Deleting epoch store");
            return Task.FromResult(EpochOperationResult.Success);
        }
        #endregion
    }
}
