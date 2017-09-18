using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Refactor
{
    class JobServices
    {
        ConcurrentDictionary<string, WorkerData> _partitionKeyRangeIdToWorkerMap;
        int _isShutdown = 0;

    }
    
}
