package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.annotation.Cardinality
import com.microsoft.durabletask.Task
import com.microsoft.durabletask.azurefunctions.DurableClientInput
import com.microsoft.durabletask.azurefunctions.DurableClientContext
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

import gov.cdc.dex.csv.dtos.ActivityParams
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.RouterConfiguration
import gov.cdc.dex.csv.constants.EnvironmentParams
import gov.cdc.dex.csv.services.AzureBlobServiceImpl
import gov.cdc.dex.csv.services.IBlobService

import java.io.InputStream
import java.io.BufferedReader
import java.util.logging.Level

class FnRouterEntry {

    // TODO: This Trigger needs to be replaced with whatever the ingestion router will use to send us the information
    @FunctionName("DexCsvRouter")
    fun pipelineEntry(
        @EventHubTrigger(
            cardinality = Cardinality.ONE,
            name = "msg", 
            eventHubName = "%EventHubName_Ingest%",
            connection = "EventHubConnection")
            event: AzureBlobCreateEventMessage,
            //might be array            events: Array<AzureBlobCreateEventMessage>,
        @DurableClientInput(name = "durableContext") durableContext:DurableClientContext,
        context: ExecutionContext
    ) {
        val ingestBlobConnectionString = System.getenv(EnvironmentParams.INGEST_BLOB_CONNECTION_PARAM) 
        if(ingestBlobConnectionString == null){
            throw IllegalArgumentException("${EnvironmentParams.INGEST_BLOB_CONNECTION_PARAM} Environment variable not defined")
        }
        val ingestBlobService = AzureBlobServiceImpl(ingestBlobConnectionString)
        
        val configBlobConnectionString = System.getenv(EnvironmentParams.CONFIG_BLOB_CONNECTION_PARAM) 
        if(configBlobConnectionString == null){
            throw IllegalArgumentException("${EnvironmentParams.CONFIG_BLOB_CONNECTION_PARAM} Environment variable not defined")
        }
        val configBlobService = AzureBlobServiceImpl(ingestBlobConnectionString)
        
        val baseConfigUrl = System.getenv(EnvironmentParams.BASE_CONFIG_URL) 
        if(baseConfigUrl == null){
            throw IllegalArgumentException("${EnvironmentParams.BASE_CONFIG_URL} Environment variable not defined")
        }
        
        //might be array            val event:AzureBlobCreateEventMessage = events.get(0)
        val errorMessage = FnRouter().pipelineEntry(event, durableContext, context, ingestBlobService, configBlobService, baseConfigUrl)
        if(errorMessage!=null){
            //TODO handle the error
            context.logger.log(Level.SEVERE,errorMessage)
        }
    }
}


class FnRouter {
    private val BLOB_CREATED_EVENT_TYPE = "Microsoft.Storage.BlobCreated"
    private val jsonParser = jacksonObjectMapper()

    fun pipelineEntry(
        event: AzureBlobCreateEventMessage,
        durableContext:DurableClientContext,
        context: ExecutionContext, 
        ingestBlobService:IBlobService, 
        configBlobService:IBlobService,
        baseConfigUrl:String
    ):String? {
        // ensure message received is blob creation
        if ( event.eventType != BLOB_CREATED_EVENT_TYPE) {
            return "Recieved non-created message type $event.eventType"
        }

        context.logger.log(Level.INFO,"Received BLOB_CREATED event: --> $event")

        //check required event parameters
        val id = event.id
        val ingestUrl = event.evHubData?.url
        if(id==null || ingestUrl==null){
            return "Event missing required parameter(s)"
        }

        //check for ingest file existence
        if(!ingestBlobService.exists(ingestUrl)){
            return "Ingest file not found: $ingestUrl"
        }

        //check for router file existence
        val routerConfigUrl = baseConfigUrl+"routerConfig.json"
        if(!configBlobService.exists(routerConfigUrl)){
            return "Router config file not found: $routerConfigUrl"
        }
        
        //parse router file
        val configList = try{
            val content = configBlobService.openDownloadStream(routerConfigUrl).bufferedReader().use(BufferedReader::readText)
            jsonParser.readValue<List<RouterConfiguration>>(content) 
        }catch(e:Exception){
            context.logger.log(Level.SEVERE,"Could not parse router config", e)
            return "Error parsing router config file $routerConfigUrl : ${e.message}"
        }

        //pull router config associated with the ingest file
        val ingestMetadata = ingestBlobService.getProperties(ingestUrl).metadata
        val routerConfig = pullRouterConfig(ingestMetadata, configList)
        if(routerConfig==null){
            return "Router config match not found for file $ingestUrl with metadata $ingestMetadata"
        }

        //check for orchestrator config existence
        val orchestratorConfigUrl = "${baseConfigUrl}orchestrator/${routerConfig.configurationFileName}"
        if(!configBlobService.exists(orchestratorConfigUrl)){
            return "Orchestrator config file not found: $orchestratorConfigUrl"
        }

        //parse orchestrator config
        val orchestratorConfig = try{
            val orchContent = configBlobService.openDownloadStream(orchestratorConfigUrl).bufferedReader().use(BufferedReader::readText)
            jsonParser.readValue<OrchestratorConfiguration>(orchContent) 
        }catch(e:Exception){
            context.logger.log(Level.SEVERE,"Could not parse orchestrator config", e)
            return "Error parsing orchestrator config file $routerConfigUrl : ${e.message}"
        }
        
        //build input
        val orchestratorInput = OrchestratorInput(orchestratorConfig, ActivityParams(executionId=id,originalFileUrl=ingestUrl))
        
        //start orchestrator
        val instanceId = durableContext.client.scheduleNewOrchestrationInstance("DexCsvOrchestrator",orchestratorInput);
        context.logger.log(Level.INFO, "Created new Java orchestration with instance ID = $instanceId");

        return null;
    }

    private fun pullRouterConfig(ingestMetadata:Map<String,String>, configList:List<RouterConfiguration>):RouterConfiguration?{
        val ingestMessageType = ingestMetadata.get("messageType")
        val ingestMessageVersion = ingestMetadata.get("messageVersion")
        for(config in configList){
            if(config.messageType==ingestMessageType && config.messageVersion==ingestMessageVersion){
                return config
            }
        }

        //if get to end, no config was found
        return null
    }
}
