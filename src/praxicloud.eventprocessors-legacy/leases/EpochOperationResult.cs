// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.leases
{
    /// <summary>
    /// The results of an Epoch operation
    /// </summary>
    public enum EpochOperationResult
    {
        /// <summary>
        /// The results were not known
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// The operation was performed successfully
        /// </summary>
        Success = 1,

        /// <summary>
        /// The operation was performed but had a failure
        /// </summary>
        Failure = 2,

        /// <summary>
        /// The operation was performed but there was a conflict with the existing epoch information
        /// </summary>
        Conflict = 3
    }
}
