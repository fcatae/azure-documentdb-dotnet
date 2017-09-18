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
    }
}
