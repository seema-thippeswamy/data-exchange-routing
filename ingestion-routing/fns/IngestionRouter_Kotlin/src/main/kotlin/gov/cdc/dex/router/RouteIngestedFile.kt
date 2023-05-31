package gov.cdc.dex.router

import com.google.gson.Gson

import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.BlobTrigger
import com.microsoft.azure.functions.annotation.BindingName
import com.microsoft.azure.functions.ExecutionContext
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.core.util.BinaryData

import java.io.File

import gov.cdc.dex.router.dtos.RouteConfig

class RouteIngestedFile {

    @FunctionName("DexCsvDecompressor")
    fun run(
        @BlobTrigger(name = "file",
               dataType = "binary",
               path = "filedump/{name}",
               connection = "BlobtriggerConnection") myBlob:ByteArray ,
        @BindingName("name")  name:String,
        context: ExecutionContext
    ) {
        context.logger.info("Blob trigger function processed blob\nName: $name\nSize: ${myBlob.size} Bytes")

        // Get file extension
        val fileExtension = File(name).extension
        if (fileExtension.isNullOrEmpty()) {
            context.logger.severe("File named $name has no extension and cannot be routed.")
            return
        }

        // Load config
        val configJson = this::class.java.classLoader.getResource("fileconfigs.json")?.readText() // Assuming the fileconfigs.json is in the resources directory
        if(configJson.isNullOrEmpty()){
            context.logger.severe("Config file is missing or empty, routing cannot continue.")
        }
        val routeConfigs = Gson().fromJson(configJson, Array<RouteConfig>::class.java).toList()

        // Get config for this extension
        var routeConfig = routeConfigs.firstOrNull { it.fileType.equals(fileExtension, ignoreCase = true) }
        if (routeConfig == null) {
            context.logger.warning("No routing configured for files with a $fileExtension extension. Will route to misc folder.")
            routeConfig = routeConfigs.firstOrNull { it.fileType == "?" }
            if(routeConfig == null){
                context.logger.severe("Config file does not define misc folder.")
                return
            }
        }

        // Define destination
        val destinationRoute = routeConfig.stagingLocations.destinationContainer
        val destinationBlobName = "$destinationRoute/$name"
        val destinationContainerName = "routedfiles"
        val destinationBlobConnection = System.getenv("DestinationBlobStorageConnection")

        val blobServiceClient = BlobServiceClientBuilder().connectionString(destinationBlobConnection).buildClient();
        val containerClient = blobServiceClient.getBlobContainerClient(destinationContainerName)

        // Create new empty blob
        val blobClient = containerClient.getBlobClient(destinationBlobName)

        // Upload the file
        blobClient.upload(BinaryData.fromBytes(myBlob))

        context.logger.info("Blob $name has been routed to $destinationBlobName")
    }
}