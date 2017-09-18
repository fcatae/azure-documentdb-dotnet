using DocumentDB.ChangeFeedProcessor.DocumentLeaseStore;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Refactor
{
    class DocDb
    {
        const string LSNPropertyName = "_lsn";

        private DocumentClient _documentClient;
        private string _collectionSelfLink;
        private int _documentCount;
        private string _collectionName;
        private string _databaseResourceId;
        private string _collectionResourceId;

        public DocDb()
        {
        }

        public string DatabaseResourceId { get => _databaseResourceId; }
        public string CollectionResourceId { get => _collectionResourceId; }
        public int DocumentCount { get => _documentCount; }
        public string CollectionName { get => _collectionName; }
        
        public async Task InitializeAsync(DocumentCollectionInfo collectionLocation)
        {
            var documentClient = new DocumentClient(collectionLocation.Uri, collectionLocation.MasterKey, collectionLocation.ConnectionPolicy);

            Uri databaseUri = UriFactory.CreateDatabaseUri(collectionLocation.DatabaseName);
            Database database = await documentClient.ReadDatabaseAsync(databaseUri);

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(collectionLocation.DatabaseName, collectionLocation.CollectionName);
            ResourceResponse<DocumentCollection> collectionResponse = await documentClient.ReadDocumentCollectionAsync(
                collectionUri,
                new RequestOptions { PopulateQuotaInfo = true });
            DocumentCollection collection = collectionResponse.Resource;

            this._databaseResourceId = database.ResourceId;
            this._collectionResourceId = collection.ResourceId;
            this._documentClient = documentClient;
            this._collectionSelfLink = collection.SelfLink;
            this._documentCount = GetDocumentCount(collectionResponse);
            this._collectionName = collectionLocation.CollectionName;
        }

        public async Task<Dictionary<string, PartitionKeyRange>> ListPartitionRange()
        {
            var ranges = new Dictionary<string, PartitionKeyRange>();
            foreach (var range in await EnumPartitionKeyRangesAsync())
                {
                ranges.Add(range.Id, range);
            }

            return ranges;
        }

        public async Task<List<PartitionKeyRange>> EnumPartitionKeyRangesAsync()
        {
            string collectionSelfLink = _collectionSelfLink;

            Debug.Assert(!string.IsNullOrWhiteSpace(collectionSelfLink), "collectionSelfLink");

            string partitionkeyRangesPath = string.Format(CultureInfo.InvariantCulture, "{0}/pkranges", collectionSelfLink);

            FeedResponse<PartitionKeyRange> response = null;
            var partitionKeyRanges = new List<PartitionKeyRange>();
            do
            {
                FeedOptions feedOptions = new FeedOptions { MaxItemCount = 1000, RequestContinuation = response != null ? response.ResponseContinuation : null };
                response = await this._documentClient.ReadPartitionKeyRangeFeedAsync(partitionkeyRangesPath, feedOptions);
                partitionKeyRanges.AddRange(response);
            }
            while (!string.IsNullOrEmpty(response.ResponseContinuation));

            return partitionKeyRanges;
        }

        public IDocumentQuery<Document> CreateDocumentChangeFeedQuery(ChangeFeedOptions options)
        {
            return _documentClient.CreateDocumentChangeFeedQuery(_collectionSelfLink, options);
        }

        public async Task<long> GetEstimatedRemainingWork(DocumentServiceLease existingLease)
        {
            long remaining = 0;

            ChangeFeedOptions options = new ChangeFeedOptions
            {
                MaxItemCount = 1
            };

            options.PartitionKeyRangeId = existingLease.PartitionId;
            options.RequestContinuation = existingLease.ContinuationToken;
            IDocumentQuery<Document> query = this._documentClient.CreateDocumentChangeFeedQuery(_collectionSelfLink, options);
            FeedResponse<Document> response = null;

            try
            {
                response = await query.ExecuteNextAsync<Document>();
                long parsedLSNFromSessionToken = TryConvertToNumber(ParseAmountFromSessionToken(response.SessionToken));
                long lastSequenceNumber = response.Count > 0 ? TryConvertToNumber(response.First().GetPropertyValue<string>(LSNPropertyName)) : parsedLSNFromSessionToken;
                long partitionRemaining = parsedLSNFromSessionToken - lastSequenceNumber;
                remaining += partitionRemaining < 0 ? 0 : partitionRemaining;
            }
            catch (DocumentClientException ex)
            {
                ExceptionDispatchInfo exceptionDispatchInfo = ExceptionDispatchInfo.Capture(ex);
                DocumentClientException dcex = (DocumentClientException)exceptionDispatchInfo.SourceException;
                if ((StatusCode.NotFound == (StatusCode)dcex.StatusCode && SubStatusCode.ReadSessionNotAvailable != (SubStatusCode)GetSubStatusCode(dcex))
                    || StatusCode.Gone == (StatusCode)dcex.StatusCode)
                {
                    // We are not explicitly handling Splits here to avoid any collision with an Observer that might have picked this up and managing the split
                    TraceLog.Error(string.Format("GetEstimateWork > Partition {0}: resource gone (subStatus={1}).", existingLease.PartitionId, GetSubStatusCode(dcex)));
                }
                else if (StatusCode.TooManyRequests == (StatusCode)dcex.StatusCode ||
                                StatusCode.ServiceUnavailable == (StatusCode)dcex.StatusCode)
                {
                    TraceLog.Warning(string.Format("GetEstimateWork > Partition {0}: retriable exception : {1}", existingLease.PartitionId, dcex.Message));
                }
                else
                {
                    TraceLog.Error(string.Format("GetEstimateWork > Partition {0}: Unhandled exception", ex.Error.Message));
                }
            }

            return remaining;
        }

        private static long TryConvertToNumber(string number)
        {
            if (string.IsNullOrEmpty(number))
            {
                return 0;
            }

            long parsed = 0;
            if (!long.TryParse(number, NumberStyles.Any, CultureInfo.InvariantCulture, out parsed))
            {
                TraceLog.Warning(string.Format(CultureInfo.InvariantCulture, "Cannot parse number '{0}'.", number));
                return 0;
            }

            return parsed;
        }

        private static string ParseAmountFromSessionToken(string sessionToken)
        {
            if (string.IsNullOrEmpty(sessionToken))
            {
                return string.Empty;
            }

            int separatorIndex = sessionToken.IndexOf(':');
            return sessionToken.Substring(separatorIndex + 1);
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

        private static int GetDocumentCount(ResourceResponse<DocumentCollection> response)
        {
            Debug.Assert(response != null);

            var resourceUsage = response.ResponseHeaders["x-ms-resource-usage"];
            if (resourceUsage != null)
            {
                var parts = resourceUsage.Split(';');
                foreach (var part in parts)
                {
                    var name = part.Split('=');
                    if (string.Equals(name[0], "documentsCount", StringComparison.OrdinalIgnoreCase))
                    {
                        return int.Parse(name[1]);
                    }
                }
            }

            return -1;
        }

    }
}
