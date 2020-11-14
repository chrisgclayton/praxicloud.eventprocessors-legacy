// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A store for lease epochs
    /// </summary>
    public interface IEpochRecorder
    {
        #region Methods
        /// <summary>
        /// Initializes the epoch reader, must be called before any operations
        /// </summary>
        /// <param name="eventProcessorHostName">The event processor host name</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if initialization is successful</returns>
        Task<bool> InitializeAsync(string eventProcessorHostName, CancellationToken cancellationToken);

        /// <summary>
        /// Add or update the epoch value to the store
        /// </summary>
        /// <param name="value">The epoch to store</param>
        /// <param name="force">true if the value should overwrite any existing ones, without using concurrency validation</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the epoch could be added or updated, false if it failed. If a violation occurs a conflict will be returned</returns>
        Task<EpochOperationResult> AddOrUpdateEpochAsync(EpochValue value, bool force, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes an existing epoch value from the store
        /// </summary>
        /// <param name="value">The epoch to delete</param>
        /// <param name="force">true if the value should delete any existing ones without using concurrency validation</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the epoch could be deleted or did not exist. If a violation occurs a conflict will be returned</returns>
        Task<EpochOperationResult> DeleteEpochAsync(EpochValue value, bool force, CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves the epoch value for the specified partition
        /// </summary>
        /// <param name="partitionId">The partition id to retrieve the epoch value for</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>An epoch response that includes the operation result within it</returns>
        Task<EpochValue> GetEpochAsync(string partitionId, CancellationToken cancellationToken);

        /// <summary>
        /// Creates the epoch store
        /// </summary>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>The outcome of the operation</returns>
        public Task<EpochOperationResult> CreateEpochStoreIfNotExistsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Deletes the epoch store
        /// </summary>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>The outcome of the operation</returns>
        public Task<EpochOperationResult> DeleteEpochStoreAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Checks if the Epoch store works
        /// </summary>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if it exists</returns>
        public Task<bool> EpochStoreExistsAsync(CancellationToken cancellationToken);
        #endregion
    }
}
