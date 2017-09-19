using DocumentDB.ChangeFeedProcessor.DocumentLeaseStore;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Refactor
{
    class JobServices
    {
        ConcurrentDictionary<string, WorkerData> _partitionKeyRangeIdToWorkerMap;
        int _isShutdown = 0;
        int _partitionCount = 0;

        private Refactor.DocDb _docdb;
        private Refactor.CheckpointServices _checkpointSvcs;

        void InterlockedIncrement()
        {
#if DEBUG
            Interlocked.Increment(ref this._partitionCount);
#endif
        }
                
        async Task OnPartitionAcquiredAsync(
            Refactor.DocDb docdb,
            Refactor.CheckpointServices checkpointServices,
            TimeSpan feedPollDelay,

            IChangeFeedObserver observer,
            DocumentServiceLease lease,
            
            Func<ChangeFeedObserverCloseReason, Task> stopAsync,
            Action<string, bool, ChangeFeedObserverCloseReason> releasePartitionAsync,
            Func<string, string, string, Task<bool>> handleSplitAsync
            // PartitionManager<DocumentServiceLease> partitionManager,
            )
        {
            ChangeFeedObserverContext context = new ChangeFeedObserverContext { PartitionKeyRangeId = lease.PartitionId };
            CancellationTokenSource cancellation = new CancellationTokenSource();
            ChangeFeedOptions options = docdb.GetChangeFeedOptions(lease.PartitionId, lease.ContinuationToken);

            var workerTask = await Task.Factory.StartNew(async () =>
            {
                ChangeFeedObserverCloseReason? closeReason = null;
                try
                {
                    try
                    {
                        await observer.OpenAsync(context);
                    }
                    catch (Exception ex)
                    {
                        TraceLog.Error(string.Format("IChangeFeedObserver.OpenAsync exception: {0}", ex));
                        closeReason = ChangeFeedObserverCloseReason.ObserverError;
                        throw;
                    }

                    CheckpointStats checkpointStats = _checkpointSvcs.GetLastCheckpointStats(lease.PartitionId);

                    IDocumentQuery<Document> query = docdb.CreateDocumentChangeFeedQuery(options);

                    TraceLog.Verbose(string.Format("Worker start: partition '{0}', continuation '{1}'", lease.PartitionId, lease.ContinuationToken));

                    string lastContinuation = options.RequestContinuation;

                    try
                    {
                        while (this._isShutdown == 0)
                        {
                            do
                            {
                                ExceptionDispatchInfo exceptionDispatchInfo = null;
                                FeedResponse<Document> response = null;

                                try
                                {
                                    response = await query.ExecuteNextAsync<Document>();
                                    lastContinuation = response.ResponseContinuation;
                                }
                                catch (DocumentClientException ex)
                                {
                                    exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                                }

                                if (exceptionDispatchInfo != null)
                                {
                                    DocumentClientException dcex = (DocumentClientException)exceptionDispatchInfo.SourceException;

                                    if (StatusCode.NotFound == (StatusCode)dcex.StatusCode && SubStatusCode.ReadSessionNotAvailable != (SubStatusCode)GetSubStatusCode(dcex))
                                    {
                                        // Most likely, the database or collection was removed while we were enumerating.
                                        // Shut down. The user will need to start over.
                                        // Note: this has to be a new task, can't await for shutdown here, as shudown awaits for all worker tasks.
                                        TraceLog.Error(string.Format("Partition {0}: resource gone (subStatus={1}). Aborting.", context.PartitionKeyRangeId, GetSubStatusCode(dcex)));
                                        await Task.Factory.StartNew(()=>stopAsync(ChangeFeedObserverCloseReason.ResourceGone));
                                        break;
                                    }
                                    else if (StatusCode.Gone == (StatusCode)dcex.StatusCode)
                                    {
                                        SubStatusCode subStatusCode = (SubStatusCode)GetSubStatusCode(dcex);
                                        if (SubStatusCode.PartitionKeyRangeGone == subStatusCode)
                                        {
                                            bool isSuccess = await handleSplitAsync(context.PartitionKeyRangeId, lastContinuation, lease.Id);
                                            if (!isSuccess)
                                            {
                                                TraceLog.Error(string.Format("Partition {0}: HandleSplit failed! Aborting.", context.PartitionKeyRangeId));
                                                await Task.Factory.StartNew(() => stopAsync(ChangeFeedObserverCloseReason.ResourceGone));
                                                break;
                                            }

                                            // Throw LeaseLostException so that we take the lease down.
                                            throw new LeaseLostException(lease, exceptionDispatchInfo.SourceException, true);
                                        }
                                        else if (SubStatusCode.Splitting == subStatusCode)
                                        {
                                            TraceLog.Warning(string.Format("Partition {0} is splitting. Will retry to read changes until split finishes. {1}", context.PartitionKeyRangeId, dcex.Message));
                                        }
                                        else
                                        {
                                            exceptionDispatchInfo.Throw();
                                        }
                                    }
                                    else if (StatusCode.TooManyRequests == (StatusCode)dcex.StatusCode ||
                                        StatusCode.ServiceUnavailable == (StatusCode)dcex.StatusCode)
                                    {
                                        TraceLog.Warning(string.Format("Partition {0}: retriable exception : {1}", context.PartitionKeyRangeId, dcex.Message));
                                    }
                                    else
                                    {
                                        exceptionDispatchInfo.Throw();
                                    }

                                    await Task.Delay(dcex.RetryAfter != TimeSpan.Zero ? dcex.RetryAfter : feedPollDelay, cancellation.Token);
                                }

                                if (response != null)
                                {
                                    if (response.Count > 0)
                                    {
                                        List<Document> docs = new List<Document>();
                                        docs.AddRange(response);

                                        try
                                        {
                                            context.FeedResponse = response;
                                            await observer.ProcessChangesAsync(context, docs);
                                        }
                                        catch (Exception ex)
                                        {
                                            TraceLog.Error(string.Format("IChangeFeedObserver.ProcessChangesAsync exception: {0}", ex));
                                            closeReason = ChangeFeedObserverCloseReason.ObserverError;
                                            throw;
                                        }
                                        finally
                                        {
                                            context.FeedResponse = null;
                                        }
                                    }

                                    checkpointStats.ProcessedDocCount += (uint)response.Count;

                                    if (_checkpointSvcs.IsCheckpointNeeded(lease, checkpointStats))
                                    {
                                        var continuation = response.ResponseContinuation;
                                        lease = await _checkpointSvcs.CheckpointAsync(lease, continuation, context);

                                        checkpointStats.Reset();
                                    }
                                    else if (response.Count > 0)
                                    {
                                        TraceLog.Informational(string.Format("Checkpoint: not checkpointing for partition {0}, {1} docs, new continuation '{2}' as frequency condition is not met", lease.PartitionId, response.Count, response.ResponseContinuation));
                                    }
                                }
                            }
                            while (query.HasMoreResults && this._isShutdown == 0);

                            if (this._isShutdown == 0)
                            {
                                await Task.Delay(feedPollDelay, cancellation.Token);
                            }
                        } // Outer while (this.isShutdown == 0) loop.

                        closeReason = ChangeFeedObserverCloseReason.Shutdown;
                    }
                    catch (TaskCanceledException)
                    {
                        Debug.Assert(cancellation.IsCancellationRequested, "cancellation.IsCancellationRequested");
                        TraceLog.Informational(string.Format("Cancel signal received for partition {0} worker!", context.PartitionKeyRangeId));
                    }
                }
                catch (LeaseLostException ex)
                {
                    closeReason = ex.IsGone ? ChangeFeedObserverCloseReason.LeaseGone : ChangeFeedObserverCloseReason.LeaseLost;
                }
                catch (Exception ex)
                {
                    TraceLog.Error(string.Format("Partition {0} exception: {1}", context.PartitionKeyRangeId, ex));
                    if (!closeReason.HasValue)
                    {
                        closeReason = ChangeFeedObserverCloseReason.Unknown;
                    }
                }

                if (closeReason.HasValue)
                {
                    TraceLog.Informational(string.Format("Releasing lease for partition {0} due to an error, reason: {1}!", context.PartitionKeyRangeId, closeReason.Value));

                    // Note: this has to be a new task, because OnPartitionReleasedAsync awaits for worker task.
                    await Task.Factory.StartNew(() => releasePartitionAsync(context.PartitionKeyRangeId, true, closeReason.Value));
                }

                TraceLog.Informational(string.Format("Partition {0}: worker finished!", context.PartitionKeyRangeId));
            });

            var newWorkerData = new WorkerData(workerTask, observer, context, cancellation);
            this._partitionKeyRangeIdToWorkerMap.AddOrUpdate(context.PartitionKeyRangeId, newWorkerData, (string id, WorkerData d) => { return newWorkerData; });
        }

        async Task CreateJob(
            Refactor.DocDb docdb,
            Refactor.CheckpointServices checkpointServices,
            TimeSpan feedPollDelay,

            IChangeFeedObserver observer,
            DocumentServiceLease lease,

            Func<ChangeFeedObserverCloseReason, Task> stopAsync,
            Action<string, bool, ChangeFeedObserverCloseReason> releasePartitionAsync,
            Func<string, string, string, Task<bool>> handleSplitAsync
            // PartitionManager<DocumentServiceLease> partitionManager,
            )
        {
            ChangeFeedObserverContext context = new ChangeFeedObserverContext { PartitionKeyRangeId = lease.PartitionId };
            CancellationTokenSource cancellation = new CancellationTokenSource();
            ChangeFeedOptions options = docdb.GetChangeFeedOptions(lease.PartitionId, lease.ContinuationToken);

            var workerTask = CreateWorkerTask(
                docdb, 
                checkpointServices,
                feedPollDelay,

                observer,
                lease,

                context,
                cancellation,
                options,

                stopAsync, releasePartitionAsync, handleSplitAsync);

            var newWorkerData = new WorkerData(workerTask, observer, context, cancellation);
            this._partitionKeyRangeIdToWorkerMap.AddOrUpdate(context.PartitionKeyRangeId, newWorkerData, (string id, WorkerData d) => { return newWorkerData; });
        }

        Task CreateWorkerTask(
            Refactor.DocDb docdb,
            Refactor.CheckpointServices checkpointServices,
            TimeSpan feedPollDelay,

            IChangeFeedObserver observer,
            DocumentServiceLease lease,

            ChangeFeedObserverContext context,
            CancellationTokenSource cancellation,
            ChangeFeedOptions options,

            Func<ChangeFeedObserverCloseReason, Task> stopAsync,
            Action<string, bool, ChangeFeedObserverCloseReason> releasePartitionAsync,
            Func<string, string, string, Task<bool>> handleSplitAsync
            // PartitionManager<DocumentServiceLease> partitionManager,
            )
        {
            return Task.Factory.StartNew(async () =>
            {
                ChangeFeedObserverCloseReason? closeReason = null;
                try
                {
                    try
                    {
                        await observer.OpenAsync(context);
                    }
                    catch (Exception ex)
                    {
                        TraceLog.Error(string.Format("IChangeFeedObserver.OpenAsync exception: {0}", ex));
                        closeReason = ChangeFeedObserverCloseReason.ObserverError;
                        throw;
                    }

                    CheckpointStats checkpointStats = _checkpointSvcs.GetLastCheckpointStats(lease.PartitionId);

                    IDocumentQuery<Document> query = docdb.CreateDocumentChangeFeedQuery(options);

                    TraceLog.Verbose(string.Format("Worker start: partition '{0}', continuation '{1}'", lease.PartitionId, lease.ContinuationToken));

                    string lastContinuation = options.RequestContinuation;

                    try
                    {
                        while (this._isShutdown == 0)
                        {
                            do
                            {
                                ExceptionDispatchInfo exceptionDispatchInfo = null;
                                FeedResponse<Document> response = null;

                                try
                                {
                                    response = await query.ExecuteNextAsync<Document>();
                                    lastContinuation = response.ResponseContinuation;
                                }
                                catch (DocumentClientException ex)
                                {
                                    exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                                }

                                if (exceptionDispatchInfo != null)
                                {
                                    DocumentClientException dcex = (DocumentClientException)exceptionDispatchInfo.SourceException;

                                    if (StatusCode.NotFound == (StatusCode)dcex.StatusCode && SubStatusCode.ReadSessionNotAvailable != (SubStatusCode)GetSubStatusCode(dcex))
                                    {
                                        // Most likely, the database or collection was removed while we were enumerating.
                                        // Shut down. The user will need to start over.
                                        // Note: this has to be a new task, can't await for shutdown here, as shudown awaits for all worker tasks.
                                        TraceLog.Error(string.Format("Partition {0}: resource gone (subStatus={1}). Aborting.", context.PartitionKeyRangeId, GetSubStatusCode(dcex)));
                                        await Task.Factory.StartNew(() => stopAsync(ChangeFeedObserverCloseReason.ResourceGone));
                                        break;
                                    }
                                    else if (StatusCode.Gone == (StatusCode)dcex.StatusCode)
                                    {
                                        SubStatusCode subStatusCode = (SubStatusCode)GetSubStatusCode(dcex);
                                        if (SubStatusCode.PartitionKeyRangeGone == subStatusCode)
                                        {
                                            bool isSuccess = await handleSplitAsync(context.PartitionKeyRangeId, lastContinuation, lease.Id);
                                            if (!isSuccess)
                                            {
                                                TraceLog.Error(string.Format("Partition {0}: HandleSplit failed! Aborting.", context.PartitionKeyRangeId));
                                                await Task.Factory.StartNew(() => stopAsync(ChangeFeedObserverCloseReason.ResourceGone));
                                                break;
                                            }

                                            // Throw LeaseLostException so that we take the lease down.
                                            throw new LeaseLostException(lease, exceptionDispatchInfo.SourceException, true);
                                        }
                                        else if (SubStatusCode.Splitting == subStatusCode)
                                        {
                                            TraceLog.Warning(string.Format("Partition {0} is splitting. Will retry to read changes until split finishes. {1}", context.PartitionKeyRangeId, dcex.Message));
                                        }
                                        else
                                        {
                                            exceptionDispatchInfo.Throw();
                                        }
                                    }
                                    else if (StatusCode.TooManyRequests == (StatusCode)dcex.StatusCode ||
                                        StatusCode.ServiceUnavailable == (StatusCode)dcex.StatusCode)
                                    {
                                        TraceLog.Warning(string.Format("Partition {0}: retriable exception : {1}", context.PartitionKeyRangeId, dcex.Message));
                                    }
                                    else
                                    {
                                        exceptionDispatchInfo.Throw();
                                    }

                                    await Task.Delay(dcex.RetryAfter != TimeSpan.Zero ? dcex.RetryAfter : feedPollDelay, cancellation.Token);
                                }

                                if (response != null)
                                {
                                    if (response.Count > 0)
                                    {
                                        List<Document> docs = new List<Document>();
                                        docs.AddRange(response);

                                        try
                                        {
                                            context.FeedResponse = response;
                                            await observer.ProcessChangesAsync(context, docs);
                                        }
                                        catch (Exception ex)
                                        {
                                            TraceLog.Error(string.Format("IChangeFeedObserver.ProcessChangesAsync exception: {0}", ex));
                                            closeReason = ChangeFeedObserverCloseReason.ObserverError;
                                            throw;
                                        }
                                        finally
                                        {
                                            context.FeedResponse = null;
                                        }
                                    }

                                    checkpointStats.ProcessedDocCount += (uint)response.Count;

                                    if (_checkpointSvcs.IsCheckpointNeeded(lease, checkpointStats))
                                    {
                                        var continuation = response.ResponseContinuation;
                                        lease = await _checkpointSvcs.CheckpointAsync(lease, continuation, context);

                                        checkpointStats.Reset();
                                    }
                                    else if (response.Count > 0)
                                    {
                                        TraceLog.Informational(string.Format("Checkpoint: not checkpointing for partition {0}, {1} docs, new continuation '{2}' as frequency condition is not met", lease.PartitionId, response.Count, response.ResponseContinuation));
                                    }
                                }
                            }
                            while (query.HasMoreResults && this._isShutdown == 0);

                            if (this._isShutdown == 0)
                            {
                                await Task.Delay(feedPollDelay, cancellation.Token);
                            }
                        } // Outer while (this.isShutdown == 0) loop.

                        closeReason = ChangeFeedObserverCloseReason.Shutdown;
                    }
                    catch (TaskCanceledException)
                    {
                        Debug.Assert(cancellation.IsCancellationRequested, "cancellation.IsCancellationRequested");
                        TraceLog.Informational(string.Format("Cancel signal received for partition {0} worker!", context.PartitionKeyRangeId));
                    }
                }
                catch (LeaseLostException ex)
                {
                    closeReason = ex.IsGone ? ChangeFeedObserverCloseReason.LeaseGone : ChangeFeedObserverCloseReason.LeaseLost;
                }
                catch (Exception ex)
                {
                    TraceLog.Error(string.Format("Partition {0} exception: {1}", context.PartitionKeyRangeId, ex));
                    if (!closeReason.HasValue)
                    {
                        closeReason = ChangeFeedObserverCloseReason.Unknown;
                    }
                }

                if (closeReason.HasValue)
                {
                    TraceLog.Informational(string.Format("Releasing lease for partition {0} due to an error, reason: {1}!", context.PartitionKeyRangeId, closeReason.Value));

                    // Note: this has to be a new task, because OnPartitionReleasedAsync awaits for worker task.
                    await Task.Factory.StartNew(() => releasePartitionAsync(context.PartitionKeyRangeId, true, closeReason.Value));
                }

                TraceLog.Informational(string.Format("Partition {0}: worker finished!", context.PartitionKeyRangeId));
            });
        }

        private int GetSubStatusCode(DocumentClientException exception)
        {
            Debug.Assert(exception != null);

            const string SubStatusHeaderName = "x-ms-substatus";
            string valueSubStatus = exception.ResponseHeaders.Get(SubStatusHeaderName);
            if (!string.IsNullOrEmpty(valueSubStatus))
            {
                int subStatusCode = 0;
                if (int.TryParse(valueSubStatus, NumberStyles.Integer, CultureInfo.InvariantCulture, out subStatusCode))
                {
                    return subStatusCode;
                }
            }

            return -1;
        }

    }

}
