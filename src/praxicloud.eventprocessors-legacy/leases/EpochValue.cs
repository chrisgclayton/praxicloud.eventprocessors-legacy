// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    /// <summary>
    /// The epoch content
    /// </summary>
    public class EpochValue
    {
        #region Properties
        /// <summary>
        /// True if the value operation was successful
        /// </summary>
        public bool Success => Result == EpochOperationResult.Success;

        /// <summary>
        /// The raw result code of the related operation
        /// </summary>
        public EpochOperationResult Result { get; set; }

        /// <summary>
        /// The partition id that the epoch is about
        /// </summary>
        public string PartitionId { get; set; }

        /// <summary>
        /// The Epoch that the partition
        /// </summary>
        public long Epoch { get; set; }

        /// <summary>
        /// The current etag or other concurrency value
        /// </summary>
        public string ConcurrencyValue { get; set; }
        #endregion
    }
}
