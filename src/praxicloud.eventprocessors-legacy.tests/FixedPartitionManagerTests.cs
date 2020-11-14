// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.tests
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using praxicloud.eventprocessors.legacy.leases;
    #endregion

    /// <summary>
    /// Tests to validate the IoT Hub Partition Manager operations
    /// </summary>
    [TestClass]
    public class FixedPartitionManagerTests
    {
        #region Methods
        /// <summary>
        /// Tests the various settings for the number of partition managers and id of the current partition manager for proper distribution
        /// </summary>
        /// <param name="id">The 0 based index of the current manager</param>
        /// <param name="quantity">The number of partition managers in use</param>
        [DataTestMethod]
        [DataRow(0, 1)]
        [DataRow(1, 2)]
        [DataRow(2, 3)]
        [DataRow(3, 4)]
        public void DetermineOwnership(int id, int quantity)
        {
            var logger = GetLogger("testing");
            var manager = new FixedPartitionManager(logger, Environment.GetEnvironmentVariable("iotc_eventhub"), id);

           
            manager.InitializeAsync(quantity).GetAwaiter().GetResult();
            var partitions = manager.GetPartitions();
            var ownership = new Dictionary<string, bool>();


            var ownedPartitions = manager.GetOwnedPartitions();

            for(var index = 0; index < partitions.Length; index++)
            {
                var partitionId = partitions[index];
                var isOwner = manager.IsOwner(partitionId);

                ownership.Add(partitionId, isOwner);
            }

            for (var index = 0; index < partitions.Length; index++)
            {
                var partitionId = partitions[index];
                var shouldOwn = (index % quantity == id);

                Assert.IsTrue(ownership[partitionId] == shouldOwn);
            }
        }

        /// <summary>
        /// Retreives a logger with the specified name
        /// </summary>
        /// <param name="name">The name of the logger</param>
        /// <returns>An instance of a logger</returns>
        private ILogger GetLogger(string name)
        {
            var collection = new ServiceCollection();

            collection.AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Debug);
            });

            var provider = collection.BuildServiceProvider();

            return provider.GetRequiredService<ILoggerFactory>().CreateLogger(name);
        }
        #endregion
    }
}
