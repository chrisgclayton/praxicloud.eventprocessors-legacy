// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.checkpoints
{
    #region Using Clauses
    using Microsoft.Azure.EventHubs.Processor;
    #endregion

    /// <summary>
    /// A legacy event processor host checkpoint manager
    /// </summary>
    public interface IPraxiCheckpointManager : ICheckpointManager
    {
        /// <summary>
        /// Initializes the checkpoint manager for use, requiring access to the Event Processor Host details
        /// </summary>
        /// <param name="host">The Event Processor Host the lease manager is associated with</param>
        void Initialize(EventProcessorHost host);
    }
}
