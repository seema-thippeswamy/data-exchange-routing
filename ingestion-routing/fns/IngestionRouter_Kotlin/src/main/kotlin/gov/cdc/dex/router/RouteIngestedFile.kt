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

import java.io.File
import java.io.BufferedInputStream
import java.io.BufferedOutputStream

import gov.cdc.dex.router.dtos.RouteConfig
import gov.cdc.dex.router.dtos.EventSchema

class RouteIngestedFile {
    
    @FunctionName("RouteIngestedFile")
    fun run(
        @EventGridTrigger(name = "event") eventContent:EventSchema,
        context: ExecutionContext
    ) {
        context.logger.info("Function triggered for event $eventContent")

        // Load config
        val configJson = this::class.java.classLoader.getResource("fileconfigs.json")?.readText() // Assuming the fileconfigs.json is in the resources directory
        if(configJson.isNullOrEmpty()){
            context.logger.severe("Config file is missing or empty, routing cannot continue.")
        }
        val routeConfigs = Gson().fromJson(configJson, Array<RouteConfig>::class.java).toList()
        
        // Define source
        val sourceUrl = eventContent.data.url
        val sourceCredential = DefaultAzureCredentialBuilder().build();
        val sourceBlob = BlobClientBuilder().credential(sourceCredential).endpoint(sourceUrl).buildClient()

        val sourceMetadata = sourceBlob.properties.metadata
        val messageType = sourceMetadata.getOrDefault("message_type", "?")
        
        // Define destination
        // val destinationBlobName = getDestination_Extension(sourceBlob.blobName, messageType, routeConfigs, context)
        val destinationBlobName = getDestination_MessageType(sourceBlob.blobName, messageType, routeConfigs, context)
        if(destinationBlobName == null){
            return
        }
        val destinationContainerName = "routedfiles"

        // val destinationConnection = System.getenv("DestinationBlobStorageConnection")
        // val destinationBlobServiceClient = BlobServiceClientBuilder().connectionString(destinationConnection).buildClient()
        val destinationCredential = DefaultAzureCredentialBuilder().build();
        val destinationBlobEndpoint = System.getenv("DestinationBlobEndpoint")
        val destinationBlobServiceClient = BlobServiceClientBuilder().endpoint(destinationBlobEndpoint).credential(destinationCredential).buildClient()
        val destinationContainerClient = destinationBlobServiceClient.getBlobContainerClient(destinationContainerName)
        val destinationBlob = destinationContainerClient.getBlobClient(destinationBlobName)

        // Copy from source
        destinationBlob.blockBlobClient.uploadFromUrl(sourceBlob.blobUrl)

        //Set metadata
        destinationBlob.setMetadata(sourceMetadata)

        context.logger.info("Blob $sourceUrl has been routed to $destinationBlobName")
    }

    /*
    mimics the C# code as of 2023-05-31
    */
    private fun getDestination_MessageType(name:String, messageType:String, routeConfigs:List<RouteConfig>, context: ExecutionContext):String?{
        var routeConfig = routeConfigs.firstOrNull { it.messageTypes.contains(messageType) }
        if (routeConfig == null) {
            context.logger.warning("No routing configured for files with a $messageType message type. Will route to misc folder.")
            routeConfig = routeConfigs.firstOrNull { it.fileType == "?" }
            if(routeConfig == null){
                context.logger.severe("Config file does not define misc folder.")
                return null
            }
        }
        
        val destinationRoute = routeConfig.stagingLocations.destinationContainer
        return "$destinationRoute/$name"
    }

    /*
    mimics the C# code as of 2023-05-30
    - with the additional message type in the folder path
    */
    private fun getDestination_Extension(name:String, messageType:String, routeConfigs:List<RouteConfig>, context: ExecutionContext):String?{
        // Get file extension
        val fileExtension = File(name).extension
        if (fileExtension.isNullOrEmpty()) {
            context.logger.severe("File named $name has no extension and cannot be routed.")
            return null
        }

        // Get config for this extension
        var routeConfig = routeConfigs.firstOrNull { it.fileType.equals(fileExtension, ignoreCase = true) }
        if (routeConfig == null) {
            context.logger.warning("No routing configured for files with a $fileExtension extension. Will route to misc folder.")
            routeConfig = routeConfigs.firstOrNull { it.fileType == "?" }
            if(routeConfig == null){
                context.logger.severe("Config file does not define misc folder.")
                return null
            }
        }

        val destinationRoute = routeConfig.stagingLocations.destinationContainer
        return "$destinationRoute/$messageType/$name"
    }

}