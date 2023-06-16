package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.annotation.Cardinality

import com.microsoft.durabletask.Task
import com.microsoft.durabletask.TaskOrchestrationContext
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger
import com.microsoft.durabletask.azurefunctions.DurableClientInput
import com.microsoft.durabletask.azurefunctions.DurableClientContext

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.specialized.BlobInputStream
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.ToNumberPolicy
import com.google.gson.JsonSyntaxException

import gov.cdc.dex.csv.dtos.ActivityInput
import gov.cdc.dex.csv.dtos.ActivityOutput
import gov.cdc.dex.csv.dtos.ActivityParams
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.CommonInput
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorStep
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.RecursiveOrchestratorOutput
import gov.cdc.dex.csv.dtos.RecursiveOrchestratorInput
import gov.cdc.dex.csv.dtos.RouterConfiguration

import java.io.IOException
import java.io.InputStream
import java.io.BufferedInputStream
import java.util.logging.Level

class FnRouter {
    private val BLOB_CONNECTION_PARAM = "BlobConnection"
    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    private val gson = GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create()
    private val ROUTER_CONFIG_URL = "https://dexcsvdata001.blob.core.windows.net/configurations/router/routerconfig.json"
    private val ORCHESTRATOR_CONFIG_URL = "https://dexcsvdata001.blob.core.windows.net/configurations/orchestrator/"
    private val blobConnection = System.getenv(BLOB_CONNECTION_PARAM)


    // TODO: This Trigger needs to be replaced with whatever the ingestion router will use to send us the information
    @FunctionName("DexNonHL7Router")
    fun DexNonHL7Router(
        @EventHubTrigger(
            cardinality = Cardinality.ONE,
            name = "msg", 
            eventHubName = "%EventHubName_Ingest%",
            connection = "EventHubConnection")
        message: String,
        @DurableClientInput(name = "durableContext") durableContext:DurableClientContext,
        context: ExecutionContext
    ) {
        context.getLogger().log(Level.SEVERE,"Router received event: $message");

        //TODO: May be needed in future iteration

        if(message.isEmpty()){
            context.getLogger().log(Level.SEVERE, "Empty Azure message")
            //TODO: SEND FAILURE
            return
        }

        // parse the message usable objects
        val events:Array<AzureBlobCreateEventMessage> = try{
            gson.fromJson(message, Array<AzureBlobCreateEventMessage>::class.java)
        }catch(e:Exception){
            context.getLogger().log(Level.SEVERE, "Error parsing Azure message: e")
            //TODO: SEND FAILURE
            return
        }

        val event:AzureBlobCreateEventMessage = events.get(0)

        // ensure message received is blob creation
        if ( event.eventType != BLOB_CREATED) {
            context.getLogger().log(Level.SEVERE,"Recieved non-created message type $event.eventType")
            return;
        }

        context.getLogger().log(Level.INFO,"Received BLOB_CREATED event: --> $event")

        //check required event parameters
        val id = event.id
        val contentType = event.evHubData?.contentType
        val contentLength = event.evHubData?.contentLength
        val url = event.evHubData?.url

        if(id==null || contentType==null || contentLength==null || url==null){
            context.getLogger().log(Level.SEVERE,"Event missing required parameter(s)");
            //TODO: SEND FAILURE
            return;
        }

        val ingestFileName = getFilenameFromUrl(url)
        if(ingestFileName == null){
                context.getLogger().log(Level.SEVERE, "Error with event URL: $url");
                //TODO: SEND FAILURE
        }

        val sourceBlob:BlobClient = BlobClientBuilder().connectionString(blobConnection).endpoint(url).buildClient()
        
        //check for file existence
        if(!sourceBlob.exists()){
            context.getLogger().log(Level.SEVERE, "File not found: $url")
            //TODO: SEND FAILURE
        }

        //TODO: The message parse will need to be edited to conform to the message from the ingestion router when that is avail
        val routerConfigBlob:BlobClient = BlobClientBuilder().connectionString(blobConnection).endpoint(ROUTER_CONFIG_URL).buildClient()
        try{
            // Convert JSON File to Java Object

            //var reader:BlobInputStream = routerConfigBlob.openInputStream()
            //reader.read(routerConfigBytes, reader.getProperties().getBlobSize(),0)
            //val routerConfigData = routerConfigBytes.toString(Charsets.UTF_8)
            //val routerConfigs = gson.fromJson(routerconfigData, Array<FileConfiguration>::class.java)
        } catch (e:IOException) {
            context.getLogger().log(Level.SEVERE, "Error reading from $ROUTER_CONFIG_URL");
            return;
        }

        var i = 0
        var found = true //TODO: change back to false when parsing is fixed
        //TODO: this is a temporary comparison, will have to be replaced with data from the event from ingestion router
        /*val routerConfig = while(routerConfigs[i].messageType != ingestFileName.substring(0,10) && i < FileConfiguration.size()-1){
            i++
            found = true
            return routerConfigs[i]
        }*/

        if(!found){
            context.getLogger().log(Level.SEVERE, "Router config match not found: $ingestFileName.substring(0,10)")
            return;
        }

        //val orchConfigFileName = parseRouterConfig(ingestFileName,ingestFileName.substring(0,10))

        /*val orchConfigBlob:BlobClient = BlobClientBuilder().connectionString(BLOB_CONNECTION_PARAM).endpoint(ORCHESTRATOR_CONFIG_URL+orchConfigFileName);
        try{
            var orchConfigReader:BlobInputStream = orchConfigBlob.openInputStream()
            // Convert JSON File to Java Object 
            val orchConfig:OrchestratorConfiguration = gson.fromJson(orchConfigReader, OrchestratorConfiguration::class.java)
        } catch (e:IOException) {
            context.logger.log(Level.SEVERE, "Error reading from $ORCHESTRATOR_CONFIG_URL $routerConfig.configurationFileName")
            return
        }*/

        val orchestratorInput = parseOrchestratorConfig(id, url) //TODO: Modify to pass proper param (ORCHESTRATOR_CONFIG_URL+orchConfigFileName)
        val client = durableContext.getClient();
        val instanceId = client.scheduleNewOrchestrationInstance("DexCsvOrchestrator",orchestratorInput);

        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);

        return;

    }

    private fun getFilenameFromUrl(url:String):String{
        //assume url is formatted "http://domain/container/pathA/pathB/fileName.something"
        //want the return as <"pathA/pathB","fileName.something">

        val urlArray = url.split("/");
        var file = urlArray[urlArray.size-1];
        return file;
    }

    /*private fun parseRouterConfig(fileType:String,fileVersion:String):routerConfiguration{
        //TODO: Move code back down from above
    }*/

    private fun parseOrchestratorConfig(id:String,url:String):OrchestratorInput{
        //TODO: Move code back down from above, remove hard-coding below
        val steps = mutableListOf<OrchestratorStep>()
        steps.add(OrchestratorStep("1", FunctionDefinition("DexCsvDecompressor"), fanOutAfter=true))
        steps.add(OrchestratorStep("2", FunctionDefinition("DummyActivity", mapOf("configKey" to "configValue2"))))

        val config = OrchestratorConfiguration(steps, FunctionDefinition("DummyActivity", mapOf("configKey" to "configValueError")))
        val initialParams = ActivityParams(executionId=id,originalFileUrl=url)
        return OrchestratorInput(config,initialParams)
    }
}
