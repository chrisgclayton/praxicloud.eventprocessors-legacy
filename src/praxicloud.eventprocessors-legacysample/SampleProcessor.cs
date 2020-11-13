// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace praxicloud.eventprocessors.legacysample
{
    public sealed class SampleProcessor : IEventProcessor
    {
        private const int ReportInterval = 10000;

        private long _eventCounter = 0;
        private long _nextReport = 1;

        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync().ConfigureAwait(false);
            }
        }

        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"Open partition {context.PartitionId}");

            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, System.Exception error)
        {
            Console.WriteLine($"Error found on partition {context.PartitionId}, error { error.Message }");

            return Task.CompletedTask;
        }

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
    }
}


