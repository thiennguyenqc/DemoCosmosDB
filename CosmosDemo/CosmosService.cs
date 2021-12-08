using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDemo
{
    public class CosmosService
    {
        private static readonly string ConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private CosmosClient cosmosClient;
        private Database database;
        private Container container;
        private string databaseId = "cosmos-demo";
        private string containerId = "containter-demo";
        public CosmosService(string containerId, CosmosClientOptions options)
        {
            this.containerId = containerId;
            cosmosClient = new CosmosClient(ConnectionString, options);
            InitCosmosDb().Wait();
        }
        private async Task InitCosmosDb()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/PartitionKey");
        }
        public async Task<double> BulkInsertItemsAsync<T>(List<T> items, string partitionKey, int bulkExecutorLimit = 1000)
        {
            var response = new List<ItemResponse<T>>();

            PartitionKey key = new PartitionKey(partitionKey);
            int skip = 0;
            var count = 1;
            while (true)
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();

                List<T> batch = items.Skip(skip).Take(bulkExecutorLimit).ToList();
                if (batch.Count == 0) break;
                List<Task<ItemResponse<T>>> concurrentTasks = new List<Task<ItemResponse<T>>>(bulkExecutorLimit);
                foreach (var itemToInsert in batch)
                {
                    concurrentTasks.Add(container.UpsertItemAsync<T>(itemToInsert, key));
                }
                var responseItems = await Task.WhenAll(concurrentTasks);
                watch.Stop();

                Console.WriteLine($"#{count++} BulkInsert {concurrentTasks.Count} items. Request Charge {responseItems.Sum(x=>x.RequestCharge)} RUs. TotalTime: {watch.ElapsedMilliseconds}ms\n");
                response.AddRange(responseItems);
                skip += bulkExecutorLimit;
            }

            return response.Sum(x => x.RequestCharge);
        }
        public async Task InsertItemsAsync<T>(List<T> items, string partitionKey)
        {
            if (items == null || !items.Any())
            {
                return;
            }

            PartitionKey key = new PartitionKey(partitionKey);
            foreach (var item in items)
            {
                await container.CreateItemAsync<T>(item, key);
            }
        }
        public async Task<ItemResponse<T>> InsertItemAsync<T>(T item, string partitionKey)
        {
            return await container.CreateItemAsync<T>(item, new PartitionKey(partitionKey));
        }
        public async Task<ItemResponse<T>> DeleteItemAsync<T>(string id, string partitionKey)
        {
            return await container.DeleteItemAsync<T>(id, new PartitionKey(partitionKey));
        }
        public async Task<ItemResponse<T>> UpsertItemAsync<T>(T item, string partitionKey)
        {
            return await container.UpsertItemAsync(item, new PartitionKey(partitionKey));
        }
        public async Task<(IList<T>, double)> ReadItemsByQueryAsync<T>(QueryDefinition query, string partitionKey)
        {
            var requestCharge = 0D;

            FeedIterator<T> feedIterator;

            if (!string.IsNullOrEmpty(partitionKey))
            {
                feedIterator = container.GetItemQueryIterator<T>(
                query,
                null,
                new QueryRequestOptions() { PartitionKey = new PartitionKey(partitionKey) });
            }
            else
            {
                feedIterator = container.GetItemQueryIterator<T>(query);
            }

            var result = new List<T>();
            while (feedIterator.HasMoreResults)
            {
                var feedIteratorResult = await feedIterator.ReadNextAsync();
                requestCharge += feedIteratorResult.RequestCharge;
                result.AddRange(feedIteratorResult);
            }

            return (result,requestCharge);
        }
    }
}
