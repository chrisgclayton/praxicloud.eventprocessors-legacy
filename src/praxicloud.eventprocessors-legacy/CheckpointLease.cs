// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy
{
    #region Using Clauses
    using Microsoft.Azure.EventHubs.Processor;
    #endregion

    /// <summary>
    /// An accessible lease that can be used for checkpointing containing the minimum required
    /// </summary>
    public class CheckpointLease : Lease
    {
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        public CheckpointLease()
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="partitionId">The partition id</param>
        public CheckpointLease(string partitionId) : base(partitionId)
        {

        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="source">The type the instance is using for content</param>
        public CheckpointLease(Lease source) : base(source)
        {
            Offset = source.Offset;
            SequenceNumber = source.SequenceNumber;
        }
        #endregion
        #region Methods
        /// <summary>
        /// Implicitly converts the lease to a checkpoint
        /// </summary>
        /// <param name="lease"></param>
        public static implicit operator Checkpoint(CheckpointLease lease)
        {
            return new Checkpoint(lease.PartitionId, lease.Offset, lease.SequenceNumber);
        }
        #endregion
    }
}
