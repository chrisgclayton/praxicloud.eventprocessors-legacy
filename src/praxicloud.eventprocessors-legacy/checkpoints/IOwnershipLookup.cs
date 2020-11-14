// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.checkpoints
{
    #region Using Clauses
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A directory to determine ownership of a partition
    /// </summary>
    public interface IOwnershipLookup
    {
        /// <summary>
        /// Checks if the specified partition is owned by the current instance
        /// </summary>
        /// <param name="partitionId">The partition id to check for ownership</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the partition is owned</returns>
        Task<bool> IsOwnerAsync(string partitionId, CancellationToken cancellationToken);
    }
}
