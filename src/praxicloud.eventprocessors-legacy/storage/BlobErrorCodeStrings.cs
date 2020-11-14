// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.storage
{
    /// <summary>
    /// BLOB Error code strings for easier translation.
    /// </summary>
    public static class BlobErrorCodeStrings
    {
        #region Constants   
        /// <summary>
        /// Blob is already existing so the operation is not possible
        /// </summary>
        public const string BlobAlreadyExists = "BlobAlreadyExists";

        /// <summary>
        /// The lease id was not included
        /// </summary>
        public const string LeaseIdMissing = "LeaseIdMissing";

        /// <summary>
        /// The lease identified is no longer held
        /// </summary>
        public const string LeaseLost = "LeaseLost";

        /// <summary>
        /// There is already a lease on the BLOB
        /// </summary>
        public const string LeaseAlreadyPresent = "LeaseAlreadyPresent";

        /// <summary>
        /// The lease id is not possible for the operation
        /// </summary>
        public const string LeaseIdMismatchWithLeaseOperation = "LeaseIdMismatchWithLeaseOperation";

        /// <summary>
        /// The lease id is not possible for the BLOB operations
        /// </summary>
        public const string LeaseIdMismatchWithBlobOperation = "LeaseIdMismatchWithBlobOperation";
        #endregion
    }
}