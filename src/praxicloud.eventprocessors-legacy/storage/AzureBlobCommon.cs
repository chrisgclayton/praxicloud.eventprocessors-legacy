// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.storage
{
    #region Using Clauses
    using System;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// Common Azure BLOB operations
    /// </summary>
    public static class AzureBlobCommon
    {
        #region Constants
        /// <summary>
        /// The default access conditions to use when accessing BLOB storage
        /// </summary>
        public const AccessCondition DefaultAccessCondition = null;

        /// <summary>
        /// The default operations context for BLOB storage which will be null
        /// </summary>
        public const OperationContext DefaultOperationContext = null;
        #endregion
        #region Variables
        /// <summary>
        /// Azure BLOB access conditions that overwrite the existing content
        /// </summary>
        public static readonly AccessCondition OverwriteAccessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");
        #endregion
        #region Methods
        /// <summary>
        /// Handles the Azure Storage exceptions
        /// </summary>
        /// <param name="partitionId">The partition id</param>
        /// <param name="exception">The storage exception that was raised</param>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <returns>The exception that was generated or passed in</returns>
        public static Exception HandleStorageException(string partitionId, StorageException exception, ILogger logger)
        {
            Exception results = exception;

            if (exception.RequestInformation.HttpStatusCode == 409 || exception.RequestInformation.HttpStatusCode == 412) 
            {
                logger.LogError(exception, "HandleStorageException (conflict or precondition failed) - HttpStatusCode: Partition Id: {partitionId}, HttpStatusCode: {status}, errorCode: {errorCode}, errorMessage: {errorMessage}", partitionId, exception.RequestInformation.HttpStatusCode, exception.RequestInformation.ErrorCode, exception.RequestInformation.ExtendedErrorInformation?.ErrorMessage);

                if (exception.RequestInformation.ErrorCode == null || exception.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseLost || exception.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMismatchWithLeaseOperation ||
                    exception.RequestInformation.ErrorCode == BlobErrorCodeStrings.LeaseIdMismatchWithBlobOperation)
                {
                    results = new LeaseLostException(partitionId, exception);
                }
            }
            else
            {
                logger.LogError(exception, "HandleStorageException - HttpStatusCode: Partition Id: {partitionId}, HttpStatusCode: {status}, errorCode: {errorCode}, errorMessage: {errorMessage}", partitionId, exception.RequestInformation.HttpStatusCode, exception.RequestInformation.ErrorCode, exception.RequestInformation.ExtendedErrorInformation?.ErrorMessage);
            }

            return results;
        }


        /// <summary>
        /// Checks to make sure the container name is valid
        /// </summary>
        /// <param name="parameterName">The name of the parameter to validate</param>
        /// <param name="containerName">The container name</param>
        public static void ValidContainerName(string parameterName, string containerName)
        {
            try
            {
                NameValidator.ValidateContainerName(containerName);
            }
            catch (Exception)
            {
                throw new GuardException("{0} parameter must be a valid Azure Storage BLOB container name", parameterName);
            }
        }
        #endregion
    }
}
