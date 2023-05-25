import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobClient
import com.microsoft.extensions.configuration.Configuration
import com.microsoft.extensions.logging.Logger
import com.microsoft.extensions.logging.LoggerFactory
import com.microsoft.extensions.logging.LogLevel
import com.microsoft.extensions.logging.LoggerFactoryExtensions.logError
import com.microsoft.extensions.logging.LoggerFactoryExtensions.logInformation
import java.io.File
import java.io.InputStream

class RouteIngestedFile(private val configuration: Configuration) {
private val logger: Logger = LoggerFactory.getLogger(RouteIngestedFile::class.java)



suspend fun run(myBlob: InputStream, name: String) {
    logger.logInformation("Blob trigger function processed blob\nName: $name\nSize: ${myBlob.available()} Bytes")

    // Get file extension
    val fileExtension = File(name).extension
    if (fileExtension.isNullOrEmpty()) {
        logger.logError("File named $name has no extension and cannot be routed.")
        return
    }

    // Load config
    val configPath = "fileconfigs.json" // Assuming the fileconfigs.json is in the same directory
    val configJson = File(configPath).readText()
    if (configJson.isNullOrEmpty()) {
        logger.logError("Config file is empty, routing cannot continue")
        return
    }
    val routeConfigs = Gson().fromJson(configJson, Array<RouteConfig>::class.java).toList()

    // Get config for this extension
    val routeConfig = routeConfigs.firstOrNull { it.fileType.equals(fileExtension, ignoreCase = true) }
        ?: routeConfigs.firstOrNull { it.fileType == "?" }
    if (routeConfig == null) {
        logger.logError("No routing configured for files with a $fileExtension extension. Will route to misc folder.")
    }

    // Define destination
    val destinationRoute = routeConfig?.stagingLocations?.get("DestinationContainer")
    val destinationBlobName = "$destinationRoute/$name"
    val destinationContainerName = "routedfiles"
    val blobServiceClient = BlobServiceClient(configuration["DestinationBlobStorageConnection"])
    val containerClient = blobServiceClient.getBlobContainerClient(destinationContainerName)

    // Build source reference
    val blobClient = containerClient.getBlobClient(destinationBlobName)

    // Upload the file
    blobClient.upload(myBlob, myBlob.available().toLong())

    logger.logInformation("Blob $name has been routed to $destinationBlobName")
}
}