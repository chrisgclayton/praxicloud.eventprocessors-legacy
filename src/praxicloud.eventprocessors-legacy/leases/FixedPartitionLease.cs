// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using Microsoft.Azure.EventHubs.Processor;
    using System;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A lease object used by kubernetes lease management
    /// </summary>
    public class FixedPartitionLease : Lease
    {
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        public FixedPartitionLease() : base()
        {
            ExpirationTime = DateTime.MinValue;
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="partitionId">The event hub partition id the lease is associated with</param>
        public FixedPartitionLease(string partitionId) : base(partitionId)
        {
            ExpirationTime = DateTime.MinValue;
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="source">The lease that this lease is based on</param>
        public FixedPartitionLease(FixedPartitionLease source) : base(source)
        {
            IsOwner = source.IsOwner;
            ExpirationTime = source.ExpirationTime;
        }
        #endregion
        #region Properties
        /// <summary>
        /// True if the current stateful set index is the owner of the partition
        /// </summary>
        public bool IsOwner { get; internal set; }

        /// <summary>
        /// The time the lease expires
        /// </summary>
        public DateTime ExpirationTime { get; internal set; }

        /// <summary>
        /// True if the lease should be considered expired and can be claimed
        /// </summary>
        public bool IsLeaseExpired => ExpirationTime < DateTime.UtcNow;
        #endregion
        #region Methods
        /// <summary>
        /// Performs a deep copy of the lease
        /// </summary>
        /// <returns>A clone of the current lease instance</returns>
        internal FixedPartitionLease Clone()
        {
            return new FixedPartitionLease()
            {
                Epoch = Epoch,
                ExpirationTime = ExpirationTime,
                IsOwner = IsOwner,
                Offset = Offset,
                Owner = Owner,
                PartitionId = PartitionId,
                SequenceNumber = SequenceNumber,
                Token = Token                
            };
        }
        #endregion
        #region Lease Overrides
        /// <inheritdocs cref="Lease" />
        public override Task<bool> IsExpired()
        {
            return Task.FromResult(IsLeaseExpired);
        }
        #endregion
    }
}
