using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor
{
    class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Test program");

            DocumentFeedObserver.StartChangeFeedHost().Wait();
        }
    }

    class DocumentFeedObserver : IChangeFeedObserver
    {
        private static int s_totalDocs = 0;
        public Task OpenAsync(ChangeFeedObserverContext context)
        {
            Console.WriteLine("Worker opened, {0}", context.PartitionKeyRangeId);
            return Task.FromResult(true);  // Requires targeting .NET 4.6+.
        }
        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            Console.WriteLine("Worker closed, {0}", context.PartitionKeyRangeId);
            return Task.FromResult(true);
        }
        public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            Console.WriteLine("Change feed: total {0} doc(s)", Interlocked.Add(ref s_totalDocs, docs.Count));
            return Task.FromResult(true);
        }

        public static async Task StartChangeFeedHost()
        {
            string hostName = Guid.NewGuid().ToString();
            DocumentCollectionInfo documentCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri("https://YOUR_SERVICE.documents.azure.com:443/"),
                MasterKey = "YOUR_SECRET_KEY==",
                DatabaseName = "db1",
                CollectionName = "documents"
            };
            DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri("https://YOUR_SERVICE.documents.azure.com:443/"),
                MasterKey = "YOUR_SECRET_KEY==",
                DatabaseName = "db1",
                CollectionName = "leases"
            };
            Console.WriteLine("Main program: Creating ChangeFeedEventHost...");
            ChangeFeedEventHost host = new ChangeFeedEventHost(hostName, documentCollectionLocation, leaseCollectionLocation);
            await host.RegisterObserverAsync<DocumentFeedObserver>();
            Console.WriteLine("Main program: press Enter to stop...");
            Console.ReadLine();
            await host.UnregisterObserversAsync();
        }

    }

}
