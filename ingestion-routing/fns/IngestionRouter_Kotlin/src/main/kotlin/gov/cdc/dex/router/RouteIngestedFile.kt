package gov.cdc.dex.router

import com.google.gson.Gson

import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.BlobTrigger
import com.microsoft.azure.functions.annotation.BindingName
import com.microsoft.azure.functions.annotation.EventGridTrigger
import com.microsoft.azure.functions.ExecutionContext
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobClientBuilder
import com.azure.core.util.BinaryData
import com.azure.core.credential.TokenCredential
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.sas.BlobSasPermission
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues

import java.io.File
import java.io.BufferedInputStream
import java.io.BufferedOutputStream

import gov.cdc.dex.router.dtos.RouteConfig
import gov.cdc.dex.router.dtos.EventSchema
import java.time.OffsetDateTime
import java.time.ZoneOffset

class RouteIngestedFile {
    
    @FunctionName("RouteIngestedFile")
    fun run(
        @EventGridTrigger(name = "event") eventContent:EventSchema,
        context: ExecutionContext
    ) {
        context.logger.info("Function triggered for event $eventContent")
        
        // Define source (ConnectionString Version)
        //val sourceUrl = eventContent.data.url
        //val blobServiceClient = BlobServiceClientBuilder().connectionString(System.getenv("BlobIngestConnectionString")).buildClient();
        //val containerClient = blobServiceClient.getBlobContainerClient(System.getenv("BlobIngestContainerName"));
        //val fileName = java.net.URI(sourceUrl).path.split("/").last()
        //val sourceBlob = containerClient.getBlobClient(fileName);

        //Define source (ServicePrincipal Version)
        val sourceUrl = eventContent.data.url
        val sourceCredential = DefaultAzureCredentialBuilder().build();
        val sourceBlob = BlobClientBuilder().credential(sourceCredential).endpoint(sourceUrl).buildClient()

        val sourceMetadata = sourceBlob.properties.metadata
        val messageType = sourceMetadata.getOrDefault("message_type", "?")

        // Load configs
        val configJson = this::class.java.classLoader.getResource("fileconfigs.json")?.readText() // Assuming the fileconfigs.json is in the resources directory
        if(configJson.isNullOrEmpty()){
            context.logger.severe("Config file is missing or empty, routing cannot continue.")
        }
        val routeConfigs = Gson().fromJson(configJson, Array<RouteConfig>::class.java).toList()

        // Find associated config
        var routeConfig = routeConfigs.firstOrNull { it.messageTypes.contains(messageType) }
        if (routeConfig == null) {
            context.logger.warning("No routing configured for files with a $messageType message type. Will route to misc folder.")
            routeConfig = routeConfigs.firstOrNull { it.fileType == "?" }
            if(routeConfig == null){
                context.logger.severe("Config file does not define misc folder.")
                return
            }
        }
        
        // Define destination
        val destinationRoute = routeConfig.stagingLocations.destinationContainer
        val destinationBlobName = "$destinationRoute/${sourceBlob.blobName}"
        val destinationContainerName = System.getenv("BlobDestinationContainerName")

        //(ConnectionString Version)
        //val destinationBlobServiceClient = BlobServiceClientBuilder().connectionString((System.getenv("BlobDestinationConnectionString"))).buildClient();
        //val destinationContainerClient = destinationBlobServiceClient.getBlobContainerClient(destinationContainerName)
        //val destinationBlob = destinationContainerClient.getBlobClient(destinationBlobName)

        //(ServicePrincipal Version)
        val destinationCredential = DefaultAzureCredentialBuilder().build();
        val destinationBlobConnection = System.getenv("BlobDestinationConnectionString")
        val destinationBlobServiceClient = BlobServiceClientBuilder().endpoint(destinationBlobConnection).credential(destinationCredential).buildClient()
        val destinationContainerClient = destinationBlobServiceClient.getBlobContainerClient(destinationContainerName)
        val destinationBlob = destinationContainerClient.getBlobClient(destinationBlobName)

        //Generate SAS (ConnectionString Version)
        val sasToken = sourceBlob.generateSas(BlobServiceSasSignatureValues(OffsetDateTime.now(ZoneOffset.UTC).plusHours(1), BlobSasPermission().setReadPermission(true)))
        val urlWithSAS = "${sourceBlob.blobUrl}?$sasToken"

        // Copy from source
        destinationBlob.blockBlobClient.copyFromUrl(urlWithSAS)

        //Set metadata
        sourceMetadata["system_provider"] = "DEX-ROUTING"
        destinationBlob.setMetadata(sourceMetadata)

        context.logger.info("Blob $sourceUrl has been routed to $destinationBlobName")
    }

}