// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacysample
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    #endregion

    /// <summary>
    /// An event processor host used for testing the lease and checkpoint managers
    /// </summary>
    public sealed class SampleProcessor : IEventProcessor
    {
        #region Constants
        /// <summary>
        /// The interval that the receive events is recorded at (after this number events)
        /// </summary>
        private const int ReportInterval = 10000;
        #endregion
        #region Variables
        /// <summary>
        /// The number of events that have been received
        /// </summary>
        private long _eventCounter = 0;

        /// <summary>
        /// The next report count to report at
        /// </summary>
        private long _nextReport = 1;
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Closing partition {context.PartitionId}");

            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync().ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"Open partition {context.PartitionId}");

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task ProcessErrorAsync(PartitionContext context, System.Exception error)
        {
            Console.WriteLine($"Error found on partition {context.PartitionId}, error { error.Message }");

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            if (messages != null)
            {
                var messageList = messages.ToArray();
                _eventCounter += messageList.Length;

                if(_eventCounter >= _nextReport)
                {
                    _nextReport = _eventCounter + ReportInterval;

                    await context.CheckpointAsync().ConfigureAwait(false);

                    Console.WriteLine($"Partition: { context.PartitionId } has received { _eventCounter } messages");
                }
            }
        }
        #endregion
    }
}


