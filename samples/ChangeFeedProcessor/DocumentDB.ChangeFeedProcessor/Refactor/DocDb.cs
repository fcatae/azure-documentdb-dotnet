using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Refactor
{
    class DocDb
    {
        private DocumentClient _documentClient;

        public DocDb(DocumentClient documentClient)
        {
            this._documentClient = documentClient;
        }

        public async Task<List<PartitionKeyRange>> EnumPartitionKeyRangesAsync(string collectionSelfLink)
        {
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
    }
}
