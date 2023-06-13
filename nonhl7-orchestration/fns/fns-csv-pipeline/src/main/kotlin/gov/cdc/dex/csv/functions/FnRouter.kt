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
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.FileConfiguration
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorStep
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.CommonInput
import gov.cdc.dex.csv.dtos.RecursiveOrchestratorOutput
import gov.cdc.dex.csv.dtos.RecursiveOrchestratorInput
import gov.cdc.dex.csv.dtos.ActivityParams

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
        context.logger.log(Level.SEVERE,"Router received event: $message");

        //TODO: May be needed in future iteration
        //val requiredMetadataFields = context.requiredMetadataFields.lowercase().split(",")

        if(message.isEmpty()){
            context.logger.log(Level.SEVERE, "Empty Azure message")
            //TODO: SEND FAILURE
            return
        }

        // parse the message usable objects
        val events:Array<AzureBlobCreateEventMessage> = try{
            gson.fromJson(message, Array<AzureBlobCreateEventMessage>::class.java)
        }catch(e:Exception){
            context.logger.log(Level.SEVERE, "Error parsing Azure message: e")
            //TODO: SEND FAILURE
            return
        }

        val event:AzureBlobCreateEventMessage = events.get(0)

        // ensure message received is blob creation
        if ( event.eventType != BLOB_CREATED) {
            context.logger.log(Level.SEVERE,"Recieved non-created message type $event.eventType")
            return;
        }

        context.logger.info("Received BLOB_CREATED event: --> $event")

        //check required event parameters
        val id = event.id
        val contentType = event.evHubData?.contentType
        val contentLength = event.evHubData?.contentLength
        val url = event.evHubData?.url

        if(id==null || contentType==null || contentLength==null || url==null){
            context.logger.log(Level.SEVERE,"Event missing required parameter(s)");
            //TODO: SEND FAILURE
            return;
        }

        //define paths in the containers (use event ID as top directory in Processed and Error)
        val pathPair = getPathFromUrl(url)
        if(pathPair == null){
                context.logger.log(Level.SEVERE, "Error with event URL: $url");
                //TODO: SEND FAILURE
        }

        //val (ingestFolder,ingestFileName) = pathPair
        //val ingestFilePath = ingestFolder+"/"+ingestFileName

        val blobConnection = System.getenv(BLOB_CONNECTION_PARAM)
        val sourceBlob:BlobClient = BlobClientBuilder().connectionString(blobConnection).endpoint(url).buildClient()
        
        //check for file existence
        if(!sourceBlob.exists()){
            context.logger.log(Level.SEVERE, "File not found: $url")
            //TODO: SEND FAILURE
        }

        //TODO: The message parse will need to be edited to conform to the message from the ingestion router when that is avail
        val routerConfigBlob:BlobClient = BlobClientBuilder().connectionString(blobConnection).endpoint(ROUTER_CONFIG_URL).buildClient()
        try{
            // Convert JSON File to Java Object

            //var reader:BlobInputStream = routerConfigBlob.openInputStream()
            //reader.read(fileConfigBytes, reader.getProperties().getBlobSize(),0)
            //val fileConfigData = fileConfigBytes.toString(Charsets.UTF_8)
            //val fileConfigs = gson.fromJson(fileconfigData, Array<FileConfiguration>::class.java)
        } catch (e:IOException) {
            context.logger.log(Level.SEVERE, "Error reading from $ROUTER_CONFIG_URL");
            return;
        }

        var i = 0
        var found = true //TODO: change back to false when parsing is fixed
        //TODO: this is a temporary comparison, will have to be replaced with data from the event from ingestion router
        /*while(Fileconfiguration[i].messageType != ingestFileName.substring(0,10) && i < FileConfiguration.size()-1){
            i++;
            found = true;
        }

        if(!found){
            context.logger.log(Level.SEVERE, "Router config match not found: $ingestFileName.substring(0,10)")
            return;
        }

        val orchConfigBlob:BlobClient = BlobClientBuilder().connectionString(BLOB_CONNECTION_PARAM).endpoint(ORCHESTRATOR_CONFIG_URL+Fileconfiguration.configurationFileName);
        try{
            var orchConfigReader:BlobInputStream = orchConfigBlob.openInputStream()
            // Convert JSON File to Java Object 
            val orchConfig:OrchestratorConfiguration = gson.fromJson(orchConfigReader, OrchestratorConfiguration::class.java)
        } catch (e:IOException) {
            context.logger.log(Level.SEVERE, "Error reading from $ORCHESTRATOR_CONFIG_URL $Fileconfiguration[i].configurationFileName");
            return;
        }*/

        val steps = mutableListOf<OrchestratorStep>()
        steps.add(OrchestratorStep("1", FunctionDefinition("DexCsvDecompressor"), fanOutAfter=true))
        steps.add(OrchestratorStep("2", FunctionDefinition("DummyActivity", mapOf("configKey" to "configValue2"))))

        val config = OrchestratorConfiguration(steps, FunctionDefinition("DummyActivity", mapOf("configKey" to "configValueError")))
        val initialParams = ActivityParams(originalFileUrl=url)
        val orchestratorInput = OrchestratorInput(config,initialParams)
        /*
        var orchSteps = List(1, OrchestratorStep)
        orchSteps[0].stepNumber = 1;
        orchSteps[0].functionToRun = FunctionDefinition("decompressor", null)

        var orchConfig:OrchestratorConfiguration
        orchConfig.steps = orchSteps

        val orchestratorInput = OrchestratorInput(orchConfig,ActivityParams(originalFileUrl=url))*/
        val client = durableContext.getClient();
        val instanceId = client.scheduleNewOrchestrationInstance("DexCsvOrchestrator",orchestratorInput);
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return;

    }

    private fun getPathFromUrl(url:String):Pair<String,String>?{
        //assume url is formatted "http://domain/container/pathA/pathB/fileName.something"
        //want the return as <"pathA/pathB","fileName.something">

        val urlArray = url.split("/");
        if(urlArray.size < 5){
            return null
        }
        var path = urlArray.subList(4, urlArray.size-1).joinToString(separator="/") 
        var file = urlArray[urlArray.size-1];
        return Pair(path,file);
    }

    /*private fun validateMetadata(metadata: Map<String,String>):String?{
        val missingKeys:MutableList<String> = mutableListOf();
        for(key in requiredMetadataFields){
            if(!key.isNullOrBlank() && metadata[key].isNullOrBlank()){
                missingKeys.add(key)
            }
        }

        if(missingKeys.isNotEmpty()){
            return "Missing required metadata key(s) : $missingKeys"
        } else {
            return null;
        }
    }*/

    private fun parseRouterConfig(){

    }

    private fun parseMessageConfig(
        taskContext:TaskOrchestrationContext, functionContext:ExecutionContext, functionName:String, activityInput:ActivityInput
    ):ActivityOutput{
        val activityOutput = try{ 
            taskContext.callActivity(functionName, activityInput, ActivityOutput::class.java).await()
        }catch(e:com.microsoft.durabletask.TaskFailedException){
            //context.logger.log(Level.SEVERE, "Error in step ${activityInput.common.stepNumber}" + e + taskContext + functionContext)
            ActivityOutput(errorMessage = e.localizedMessage, updatedParams = activityInput.common.params)
        }
        return activityOutput;
    }

    private fun buildOrchestratorInput():OrchestratorInput{
        val steps = mutableListOf<OrchestratorStep>()
        steps.add(OrchestratorStep("1", FunctionDefinition("DexCsvDecompressor"), fanOutAfter=true))
        steps.add(OrchestratorStep("2", FunctionDefinition("DummyActivity", mapOf("configKey" to "configValue2"))))

        val config = OrchestratorConfiguration(steps, FunctionDefinition("DummyActivity", mapOf("configKey" to "configValueError")))
        val initialParams = ActivityParams(originalFileUrl="https://dexcsvdata001.blob.core.windows.net/processed/test/test-upload-zip.zip")
        return OrchestratorInput(config,initialParams)
    }
}