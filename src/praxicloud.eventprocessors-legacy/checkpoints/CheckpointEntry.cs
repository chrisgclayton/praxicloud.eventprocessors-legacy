// Copyright (c) Chris Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.checkpoints
{
    #region Using Clauses
    using System;
    using Microsoft.Azure.EventHubs.Processor;
    using Newtonsoft.Json;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// EventHubs checkpoint details
    /// </summary>
    /// <remarks>Legacy BLOB checkpointing had the following format {"Offset":"12896067888","SequenceNumber":911736,"PartitionId":"0","Owner":"","Token":"","Epoch":23}</remarks>
    [Serializable]
    public sealed class CheckpointEntry 
    {
        #region Properties
        /// <summary>
        /// The event hub offset 
        /// </summary>
        public string Offset { get; set; }

        /// <summary>
        /// The event hub sequence number
        /// </summary>
        public long SequenceNumber { get; set; }

        /// <summary>
        /// The event hub partition id
        /// </summary>
        public string PartitionId { get; set; }

        /// <summary>
        /// The owner of the partition
        /// </summary>
        public string Owner { get; set; }

        /// <summary>
        /// The token used to acquire the last lease
        /// </summary>
        public string Token { get; set; }

        /// <summary>
        /// The value of the latest epoch reader
        /// </summary>
        public long Epoch { get; set; }
        #endregion
        #region Methods
        /// <summary>
        /// Returns the contents as a JSON string
        /// </summary>
        /// <returns>A JSON string representing the value of the checkpoint entry</returns>
        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }

        /// <summary>
        /// Converts the JSON string to a checkpoint entry object
        /// </summary>
        /// <param name="json">The JSON to convert</param>
        /// <returns>A checkpoint entry with the contents found in the JSON string</returns>
        public static CheckpointEntry FromJson(string json)
        {
            Guard.NotNullOrWhitespace(nameof(json), json);

            return JsonConvert.DeserializeObject<CheckpointEntry>(json);
        }

        /// <summary>
        /// Converts the checkpoint to a checkpoint entry with the specified details
        /// </summary>
        /// <param name="checkpoint">The checkpoint to convert</param>
        /// <returns>A checkpoint entry with the contents of the checkpoint</returns>
        public static implicit operator CheckpointEntry(Checkpoint checkpoint)
        {
            Guard.NotNull(nameof(checkpoint), checkpoint);

            return new CheckpointEntry
            {
                PartitionId = checkpoint.PartitionId,
                Offset = checkpoint.Offset,
                SequenceNumber = checkpoint.SequenceNumber
            };
        }

        /// <summary>
        /// Converts the checkpoint entry to a checkpoint with the specified details
        /// </summary>
        /// <param name="entry">The checkpoint entry to convert</param>
        /// <returns>A checkpoint with the contents of the checkpoint entry</returns>
        public static implicit operator Checkpoint(CheckpointEntry entry)
        {
            Guard.NotNull(nameof(entry), entry);

            return new Checkpoint(entry.PartitionId, entry.Offset, entry.SequenceNumber);
        }
        #endregion
    }
}
