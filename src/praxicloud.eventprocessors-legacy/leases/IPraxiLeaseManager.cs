// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    #region Using Clauses
    using Microsoft.Azure.EventHubs.Processor;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A legacy event processor host lease manager
    /// </summary>
    public interface IPraxiLeaseManager : ILeaseManager
    {
        #region Methods
        /// <summary>
        /// Initializes the lease manager for use, requiring access to the Event Processor Host details
        /// </summary>
        /// <param name="host">The Event Processor Host the lease manager is associated with</param>
        Task InitializeAsync(EventProcessorHost host);
        #endregion
    }
}
