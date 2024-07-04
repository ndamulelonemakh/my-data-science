using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

var sourceClient = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONNECTION_STRING__SOURCE"), new CosmosClientOptions { AllowBulkExecution = true });
var targetClient = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONNECTION_STRING__TARGET"), new CosmosClientOptions { AllowBulkExecution = true });

var sourceContainer = sourceClient.GetContainer("MyDB", "MyContainer");
var destinationContainer = await CreateDestinationContainerIfNotExists(targetClient);

await ProcessDataAsync();

async Task ProcessDataAsync()
{
    string query = "SELECT * FROM c";
    FeedIterator<JObject> resultSetIterator = sourceContainer.GetItemQueryIterator<JObject>(new QueryDefinition(query));

    int pageNo = 0;

    while (resultSetIterator.HasMoreResults)
    {
        List<Task> tasks = new();
        FeedResponse<JObject> response = await resultSetIterator.ReadNextAsync();
        pageNo++;

        Console.WriteLine($"[START] Processing page {pageNo}");
        Console.WriteLine(new string('-', 50));

        foreach (JObject item in response)
        {
            tasks.Add(UpsertWithRetry(destinationContainer, item));
        }

        await Task.WhenAll(tasks);

        Console.WriteLine(new string('-', 50));
        Console.WriteLine($"[END] Processing page {pageNo}");
        Console.WriteLine();
    }
}

async Task<Container> CreateDestinationContainerIfNotExists(CosmosClient client)
{
    Database database = await client.CreateDatabaseIfNotExistsAsync("MyOtherDB");
    ContainerProperties containerProperties = new("MyOtherContainer", "/id");
    return await database.CreateContainerIfNotExistsAsync(containerProperties);
}

async Task UpsertWithRetry(Container container, JObject item)
{
    int retries = 0;
    int waitTime = 5; // seconds

    while (retries < 5)
    {
        try
        {
            await container.UpsertItemAsync(item);
            Console.WriteLine($"Item with id {item["id"]} transferred successfully.");
            return;
        }
        catch (CosmosException)
        {
            Console.WriteLine($"Error occurred while transferring item with id {item["id"]}. Retrying in {waitTime} seconds...");
            await Task.Delay(waitTime * 1000);
            waitTime *= 2; // Exponential backoff
            retries++;
        }
    }

    Console.WriteLine($"Failed to transfer item with id {item["id"]} after 5 attempts.");
}
