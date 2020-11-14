// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.checkpoints
{
    #region Using Clauses
    using System;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.containers;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// An event processor host checkpoint manager that is backed by files. 
    /// </summary>
    public class FileCheckpointManager : IPraxiCheckpointManager
    {
        #region Variables
        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Set the default encoding type
        /// </summary>
        private readonly Encoding _checkpointEncoding = Encoding.ASCII;

        /// <summary>
        /// The base directory for checkpoint partitions
        /// </summary>
        private string _baseDirectory;

        /// <summary>
        /// A partition ownership lookup directory
        /// </summary>
        private readonly IOwnershipLookup _ownershipLookup;

        /// <summary>
        /// The name of the event processor host
        /// </summary>
        private string _eventProcessorName;

        /// <summary>
        /// Metric recorder for the number of reads
        /// </summary>
        private readonly ICounter _checkpointReadCounter;

        /// <summary>
        /// Metric recorder for the number of writes
        /// </summary>
        private readonly ICounter _checkpointUpdateCounter;

        /// <summary>
        /// Metric recorder for the number of errors encountered
        /// </summary>
        private readonly ICounter _checkpointErrorCounter;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        public FileCheckpointManager(ILogger logger, IMetricFactory metricFactory, IOwnershipLookup ownershipLookup, string baseDirectory)
        {
            Guard.NotNull(nameof(ownershipLookup), ownershipLookup);
            Guard.NotNullOrWhitespace(nameof(baseDirectory), baseDirectory);
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNull(nameof(metricFactory), metricFactory);

            _logger = logger;
            _ownershipLookup = ownershipLookup;

            using (_logger.BeginScope("File Checkpoint Manager::ctor"))
            {               
                _baseDirectory = baseDirectory;
                _logger.LogInformation("Creating file checkpoint manager at { baseDirectory }", baseDirectory);

                _checkpointReadCounter = metricFactory.CreateCounter("fcm-checkpoint-read", "The number of times that the file checkpoint manager has read the checkpoint", false, new string[0]);
                _checkpointUpdateCounter = metricFactory.CreateCounter("fcm-checkpoint-update", "The number of times that the file checkpoint manager has updated the checkpoint", false, new string[0]);
                _checkpointErrorCounter = metricFactory.CreateCounter("fcm-checkpoint-error", "The number of times that the file checkpoint manager has errors raised", false, new string[0]);
            }
        }
        #endregion

        public void Initialize(EventProcessorHost host)
        {
            var fullPath = Path.Combine(_baseDirectory, host.ConsumerGroupName);

            _logger.LogInformation("Creating file checkpoint manager at { baseDirectory }", fullPath);

            _baseDirectory = fullPath;
            _eventProcessorName = host.HostName;
        }

        /// <inheritdoc />
        public Task<bool> CheckpointStoreExistsAsync()
        {
            bool exists = false;

            using (_logger.BeginScope("Store Exists"))
            {
                exists = Directory.Exists(_baseDirectory);
                _logger.LogDebug("Existance check: {exists}", exists);
            }

            return Task.FromResult(exists);
        }

        /// <inheritdoc />
        public async Task<bool> CreateCheckpointStoreIfNotExistsAsync()
        {
            bool success = false;

            using (_logger.BeginScope("Create Store"))
            {
                try
                {
                    if (!await CheckpointStoreExistsAsync().ConfigureAwait(false))
                    {
                        Directory.CreateDirectory(_baseDirectory);
                        _logger.LogInformation("Directory created");
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error creating checkpoint store");
                    _checkpointErrorCounter.Increment();
                }

                success = await CheckpointStoreExistsAsync().ConfigureAwait(false);
                _logger.LogInformation("Creation success check: {success}", success);
            }

            return success;
        }


        /// <inheritdoc />
        public async Task<Checkpoint> CreateCheckpointIfNotExistsAsync(string partitionId)
        {
            CheckpointEntry checkpoint;

            using (_logger.BeginScope("Create checkpoint"))
            {
                _checkpointUpdateCounter.Increment();
                _logger.LogDebug("Checkpoint partition {partitionId}", partitionId);

                checkpoint = await ReadCheckpointFromFileAsync(partitionId).ConfigureAwait(false);

                if (checkpoint == null)
                {
                    _logger.LogInformation("Checkpoint not found for partition {partitionId}, creating", partitionId);

                    var fileName = GetFileName(partitionId);
                    var newCheckpoint = new CheckpointEntry { PartitionId = partitionId, Owner = _eventProcessorName };
                    var json = newCheckpoint.ToJson();

                    _logger.LogTrace("Checkpoint json: ", json);

                    try
                    {
                        await File.WriteAllTextAsync(fileName, json, ContainerLifecycle.CancellationToken).ConfigureAwait(false);
                        _logger.LogDebug("Checkpoint complete for file {fileName}", fileName);
                        checkpoint = newCheckpoint;
                    }
                    catch (Exception e)
                    {
                        _logger?.LogError(e, "Error writing checkpoint file {fileName}", fileName);
                        _checkpointErrorCounter.Increment();

                    }
                }
                else
                {
                    _logger?.LogTrace("Checkpoint found for partition {partitionId}", partitionId);
                }
            }

            return checkpoint;
        }

        /// <inheritdoc />
        public Task DeleteCheckpointAsync(string partitionId)
        {
            using (_logger.BeginScope("Delete Checkpoint"))
            {
                _logger.LogInformation("Deleting checkpoint for partition {partitionId}", partitionId);

                var fileName = GetFileName(partitionId);

                try
                {
                    if (File.Exists(fileName))
                    {
                        _logger.LogTrace("Deleting file {fileName}", fileName);
                        File.Delete(fileName);
                        _logger.LogInformation("Deleted file {fileName}", fileName);
                    }
                    else
                    {
                        _logger.LogInformation("File {fileName} does not exist", fileName);
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error deleting file {fileName}", fileName);
                    _checkpointErrorCounter.Increment();
                }
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task<Checkpoint> GetCheckpointAsync(string partitionId)
        {
            Checkpoint checkpoint = null;

            using (_logger.BeginScope("Get Checkpoint"))
            {
                _logger.LogTrace("Retrieving checkpoint for partition {partitionId}", partitionId);
                checkpoint = await ReadCheckpointFromFileAsync(partitionId).ConfigureAwait(false);

                if (checkpoint == null)
                {
                    _logger.LogInformation("Checkpoint was null for partition {partitionId}", partitionId);
                }
                else
                {
                    _logger.LogTrace("Checkpoint was not null for partition {partitionId}", partitionId);
                }
            }

            return checkpoint;
        }

        /// <inheritdoc />
        public async Task UpdateCheckpointAsync(Lease lease, Checkpoint checkpoint)
        {
            var checkpointTest = false;

            using (_logger.BeginScope("Update checkpoint"))
            {
                var partitionId = checkpoint.PartitionId;
                var fileName = GetFileName(checkpoint.PartitionId);

                _logger.LogTrace("Updating checkpoint file {fileName}", fileName);

                if (_ownershipLookup == null)
                {
                    _logger.LogDebug("Proceeding in unsafe mode");
                    checkpointTest = true;
                }
                else
                {
                    _logger.LogDebug("Proceeding in safe mode");

                    if (await _ownershipLookup.IsOwnerAsync(partitionId, ContainerLifecycle.CancellationToken).ConfigureAwait(false))
                    {
                        _logger.LogDebug("Ownership established");
                        checkpointTest = true;
                    }
                    else
                    {
                        _logger.LogError("Cannot checkpoint to unowned partition {partitionId}", partitionId);
                    }
                }

                if (checkpointTest)
                {
                    var storageCheckpoint = await ReadCheckpointFromFileAsync(partitionId).ConfigureAwait(false) ?? new CheckpointEntry { PartitionId = partitionId, Owner = _eventProcessorName };

                    storageCheckpoint.Offset = checkpoint.Offset;
                    storageCheckpoint.SequenceNumber = checkpoint.SequenceNumber;

                    try
                    {
                        var json = storageCheckpoint.ToJson();
                        await File.WriteAllTextAsync(fileName, json, ContainerLifecycle.CancellationToken).ConfigureAwait(false);
                        _logger.LogTrace("Checkpoint completed: {json}", json);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Error writing checkpoint");
                        _checkpointErrorCounter.Increment();
                    }
                }

                _checkpointUpdateCounter.Increment();
            }
        }

        /// <summary>
        /// Gets the checkpoint file name for the partition id
        /// </summary>
        /// <param name="partitionId">The partition id</param>
        /// <returns>Fully qualified file name</returns>
        private string GetFileName(string partitionId)
        {
            return Path.Combine(_baseDirectory, $"partition{partitionId}.json");
        }

        /// <summary>
        /// Reads the checkpoint information from the file for partition id
        /// </summary>
        /// <param name="partitionId">The partition id the checkpoint data is for</param>
        /// <returns>A checkpoint instance populated from the file</returns>
        private async Task<CheckpointEntry> ReadCheckpointFromFileAsync(string partitionId)
        {
            CheckpointEntry result = null;

            using (_logger.BeginScope("Read Checkpoint"))
            {
                _checkpointReadCounter.Increment();
                _logger.LogDebug("Reading checkpoint partition {partitionId}", partitionId);

                var fileName = GetFileName(partitionId);

                if (File.Exists(fileName))
                {
                    _logger.LogDebug("File {fileName} exists", fileName);

                    try
                    {
                        var json = await File.ReadAllTextAsync(fileName, ContainerLifecycle.CancellationToken).ConfigureAwait(false);

                        if (string.IsNullOrWhiteSpace(json))
                        {
                            _logger.LogInformation("File {fileName} was empty", fileName);
                        }
                        else
                        {
                            _logger.LogInformation("File {fileName} had the following content: {content}", fileName, json);

                            try
                            {
                                result = CheckpointEntry.FromJson(json);
                            }
                            catch (Exception e)
                            {
                                _logger.LogError(e, "Error deserializing checkpoint contents");
                                _checkpointErrorCounter.Increment();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Error reading checkpoint file {fileName}", fileName);
                        _checkpointErrorCounter.Increment();
                    }
                }
                else
                {
                    _logger.LogInformation("File {fileName} does not exists", fileName);
                }
            }

            return result;
        }
    }
}
