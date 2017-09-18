using DocumentDB.ChangeFeedProcessor.DocumentLeaseStore;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Refactor
{
    class CheckpointServices
    {
        private CheckpointFrequency _checkpointOptions;
        private ICheckpointManager _checkpointMgr;
        ConcurrentDictionary<string, CheckpointStats> _statsSinceLastCheckpoint = new ConcurrentDictionary<string, CheckpointStats>();

        public CheckpointServices(ICheckpointManager checkpointMgr, CheckpointFrequency checkpointOptions)
        {
            this._checkpointMgr = checkpointMgr;
            this._checkpointOptions = checkpointOptions;
        }

        public bool IsCheckpointNeeded(DocumentServiceLease lease, CheckpointStats checkpointStats)
        {
            var options = new ChangeFeedHostOptions() { CheckpointFrequency = _checkpointOptions };

            Debug.Assert(lease != null);
            Debug.Assert(checkpointStats != null);

            if (checkpointStats.ProcessedDocCount == 0)
            {
                return false;
            }

            bool isCheckpointNeeded = true;

            if (options.CheckpointFrequency != null &&
                (options.CheckpointFrequency.ProcessedDocumentCount.HasValue || options.CheckpointFrequency.TimeInterval.HasValue))
            {
                // Note: if either condition is satisfied, we checkpoint.
                isCheckpointNeeded = false;
                if (options.CheckpointFrequency.ProcessedDocumentCount.HasValue)
                {
                    isCheckpointNeeded = checkpointStats.ProcessedDocCount >= options.CheckpointFrequency.ProcessedDocumentCount.Value;
                }

                if (options.CheckpointFrequency.TimeInterval.HasValue)
                {
                    isCheckpointNeeded = isCheckpointNeeded ||
                        DateTime.Now - checkpointStats.LastCheckpointTime >= options.CheckpointFrequency.TimeInterval.Value;
                }
            }

            return isCheckpointNeeded;
        }

        public async Task<DocumentServiceLease> CheckpointAsync(DocumentServiceLease lease, string continuation, ChangeFeedObserverContext context)
        {
            Debug.Assert(lease != null);
            Debug.Assert(!string.IsNullOrEmpty(continuation));

            DocumentServiceLease result = null;
            try
            {
                result = (DocumentServiceLease)await _checkpointMgr.CheckpointAsync(lease, continuation, lease.SequenceNumber + 1);

                Debug.Assert(result.ContinuationToken == continuation, "ContinuationToken was not updated!");
                TraceLog.Informational(string.Format("Checkpoint: partition {0}, new continuation '{1}'", lease.PartitionId, continuation));
            }
            catch (LeaseLostException)
            {
                TraceLog.Warning(string.Format("Partition {0}: failed to checkpoint due to lost lease", context.PartitionKeyRangeId));
                throw;
            }
            catch (Exception ex)
            {
                TraceLog.Error(string.Format("Partition {0}: failed to checkpoint due to unexpected error: {1}", context.PartitionKeyRangeId, ex.Message));
                throw;
            }

            Debug.Assert(result != null);
            return await Task.FromResult<DocumentServiceLease>(result);
        }

        public void AddOrUpdateStats(string addedRangeId)
        {
            this._statsSinceLastCheckpoint.AddOrUpdate(
            addedRangeId,
            new CheckpointStats(),
            (partitionId, existingStats) => existingStats);
        }

        public CheckpointStats TryRemoveStats(string goneRangeId)
        {
            CheckpointStats removedStatsUnused = null;
            this._statsSinceLastCheckpoint.TryRemove(goneRangeId, out removedStatsUnused);

            return removedStatsUnused;
        }

        public CheckpointStats GetLastCheckpointStats(string partitionId)
        {
            CheckpointStats checkpointStats = null;
            if (!this._statsSinceLastCheckpoint.TryGetValue(partitionId, out checkpointStats) || checkpointStats == null)
            {
                // It could be that the lease was created by different host and we picked it up.
                checkpointStats = this._statsSinceLastCheckpoint.AddOrUpdate(
                    partitionId,
                    new CheckpointStats(),
                    (existingPartitionId, existingStats) => existingStats);
                Trace.TraceWarning(string.Format("Added stats for partition '{0}' for which the lease was picked up after the host was started.", partitionId));
            }

            return checkpointStats;
        }
    }
}
