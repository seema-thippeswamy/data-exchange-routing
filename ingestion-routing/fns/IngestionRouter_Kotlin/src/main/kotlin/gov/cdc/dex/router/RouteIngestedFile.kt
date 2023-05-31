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
import com.azure.core.util.BinaryData

import java.io.File
import java.io.BufferedInputStream
import java.io.BufferedOutputStream

import gov.cdc.dex.router.dtos.RouteConfig

class RouteIngestedFile {
    
    @FunctionName("RouteIngestedFile")
    fun run_v1(
        @EventGridTrigger(name = "event") eventContent:String,
        context: ExecutionContext
    ) {
        context.logger.info("Function triggered for event $eventContent")

        // Get source file name from event
        val name = getSourceFileFromEvent(eventContent)

        // Load config
        val configJson = this::class.java.classLoader.getResource("fileconfigs.json")?.readText() // Assuming the fileconfigs.json is in the resources directory
        if(configJson.isNullOrEmpty()){
            context.logger.severe("Config file is missing or empty, routing cannot continue.")
        }
        val routeConfigs = Gson().fromJson(configJson, Array<RouteConfig>::class.java).toList()

        // Define source
        val sourceContainerName = "filedump"
        val sourceBlob = getBlob_ConnectionString("SourceBlobStorageConnection", sourceContainerName, name)
        val sourceMetadata = sourceBlob.properties.metadata
        val messageType = sourceMetadata.getOrDefault("message_type", "?")
        
        // Define destination
        val destinationBlobName = getDestination_MessageType(name, messageType, routeConfigs, context)
        if(destinationBlobName == null){
            return
        }
        val destinationContainerName = "routedfiles"
        val destinationBlob = getBlob_ConnectionString("DestinationBlobStorageConnection", destinationContainerName, destinationBlobName)

        copyBlob_ConnectionString(sourceBlob, destinationBlob)

        //Set metadata
        destinationBlob.setMetadata(sourceMetadata)

        context.logger.info("Blob $name has been routed to $destinationBlobName")
    }

    private fun getSourceFileFromEvent(event:String):String{
        return ""
    }

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

    private fun getBlob_ConnectionString(connectionParam:String, containerName:String, blobName:String):BlobClient{
        val blobConnection = System.getenv(connectionParam)
        val blobServiceClient = BlobServiceClientBuilder().connectionString(blobConnection).buildClient();
        val containerClient = blobServiceClient.getBlobContainerClient(containerName)
        return containerClient.getBlobClient(blobName)
    }

    private fun copyBlob_ConnectionString(sourceBlob:BlobClient, destinationBlob:BlobClient){
        BufferedInputStream(sourceBlob.openInputStream()).use{ fromStream ->
            BufferedOutputStream(destinationBlob.blockBlobClient.getBlobOutputStream()).use{ toStream ->
                val bytesIn = ByteArray(4096)
                var read: Int
                while (fromStream.read(bytesIn).also { read = it } != -1) {
                    toStream.write(bytesIn, 0, read)
                }
            }
        }
    }

    private fun getBlob_SasToken(sasTokenParam:String, endpointParam:String, containerName:String, blobName:String):BlobClient{
        val blobSasToken = System.getenv(sasTokenParam)
        val blobEndpoint = System.getenv(endpointParam)
        val blobServiceClient = BlobServiceClientBuilder().endpoint(blobSasToken).sasToken(blobEndpoint).buildClient()
        val containerClient = blobServiceClient.getBlobContainerClient(containerName)
        return containerClient.getBlobClient(blobName)
    }

    private fun copyBlob_SasToken(sourceBlob:BlobClient, destinationBlob:BlobClient){
        destinationBlob.blockBlobClient.uploadFromUrl(sourceBlob.blobUrl)
    }
}