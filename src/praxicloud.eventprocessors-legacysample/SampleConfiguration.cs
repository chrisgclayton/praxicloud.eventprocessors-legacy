// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacysample
{
    #region Using Clauses
    using Microsoft.Extensions.Logging;
    #endregion

    /// <summary>
    /// The configuration elements for the sample
    /// </summary>
    public sealed class SampleConfiguration
    {
        #region Properties
        /// <summary>
        /// A container used to checkpoint to
        /// </summary>
        public string CheckpointContainerName { get; set; }

        /// <summary>
        /// A container to store leases in
        /// </summary>
        public string LeaseContainerName { get; set; }

        /// <summary>
        /// A container to store epoch values in
        /// </summary>
        public string EpochContainerName { get; set; }

        /// <summary>
        /// The checkpoint connection string
        /// </summary>
        public string CheckpointConnectionString { get; set; }

        /// <summary>
        /// The lease connection string
        /// </summary>
        public string LeaseConnectionString { get; set; }

        /// <summary>
        /// The event hub connection string
        /// </summary>
        public string EventHubConnectionString { get; set; }

        /// <summary>
        /// The name of the event processor host
        /// </summary>
        public string EventProcessorHostName { get; set; }

        /// <summary>
        /// The consumer group name
        /// </summary>
        public string ConsumerGroupName { get; set; }

        /// <summary>
        /// A logger to write diagnostics information for the lease manager to
        /// </summary>
        public ILogger LeaseLogger { get; set; }

        /// <summary>
        /// The logger to write diagnostics information to for the checkpoint manager
        /// </summary>
        public ILogger CheckpointLogger { get; set; }

        /// <summary>
        /// The logger to write the sample processor information to
        /// </summary>
        public ILogger ProcessorLogger { get; set; }

        /// <summary>
        /// The logger to write partition manager information to
        /// </summary>
        public ILogger PartitionManagerLogger { get; set; }

        /// <summary>
        /// The logger to write epoch recorder information to
        /// </summary>
        public ILogger EpochRecorderLogger { get; set; }
        #endregion
    }
}
