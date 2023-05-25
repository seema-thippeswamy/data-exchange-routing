using Azure.Storage.Blobs;
using IngestionRouter.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace IngestionRouter;

public class RouteIngestedFile
{
    private readonly IConfiguration _configuration;
    public RouteIngestedFile(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    [FunctionName("RouteIngestedFile")]
    public async Task Run([BlobTrigger("filedump/{name}", Connection = "blobtriggerConnection")]Stream myBlob, string name, ILogger log, ExecutionContext context)
    {
        log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

        //Get File Extension
        string fileExtension = Path.GetExtension(name);
        if (string.IsNullOrEmpty(fileExtension) )
        {
            log.LogError($"File named {name} has no extension and cannot be routed.");
            return;
        }

        //Load Config
        string configPath = Path.Combine(context.FunctionAppDirectory, "fileconfigs.json");
        string configJson = File.ReadAllText(configPath);
        if (string.IsNullOrEmpty(configJson) )
        {
            log.LogError("Config file is empty, routing cannot continue");
            return;
        }
        List<RouteConfig> routeConfigs = JsonConvert.DeserializeObject<List<RouteConfig>>(configJson);

        //Get Config for this Extension
        RouteConfig routeConfig = routeConfigs.FirstOrDefault(x => x.FileType.ToUpper() == fileExtension.ToUpper().Replace(".",""));
        if (routeConfig is null)
        {
            log.LogError($"No routing configured for files with a {fileExtension} extension. Will route to misc folder.");
            routeConfig = routeConfigs.FirstOrDefault(x => x.FileType == "?");
        }

        //Define Destination
        string destinationRoute = routeConfig.StagingLocations["DestinationContainer"];
        string destinationBlobName = $"{destinationRoute}/{name}";
        string destinationContainerName = "routedfiles";
        BlobServiceClient blobServiceClient = new BlobServiceClient(_configuration["DestinationBlobStorageConnection"]);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(destinationContainerName);

        //Build Source reference
        BlobClient blobClient = containerClient.GetBlobClient(destinationBlobName);

        //Upload the file
        await blobClient.UploadAsync(myBlob);

        log.LogInformation($"Blob {name} has been routed to {destinationBlobName}");
    }
}
