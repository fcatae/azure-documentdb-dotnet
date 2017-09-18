using DocumentDB.ChangeFeedProcessor.DocumentLeaseStore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Refactor
{
    class CheckpointServices
    {
        private ICheckpointManager _checkpointMgr;

        public CheckpointServices(ICheckpointManager checkpointMgr)
        {
            this._checkpointMgr = checkpointMgr;
        }

        public static bool IsCheckpointNeeded(DocumentServiceLease lease, CheckpointStats checkpointStats, CheckpointFrequency checkpointOptions)
        {
            var options = new ChangeFeedHostOptions() { CheckpointFrequency = checkpointOptions };

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

    }
}
