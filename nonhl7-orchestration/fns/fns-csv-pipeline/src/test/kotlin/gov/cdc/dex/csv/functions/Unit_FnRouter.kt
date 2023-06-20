package gov.cdc.dex.csv.functions

import com.microsoft.durabletask.azurefunctions.DurableClientContext
import com.microsoft.durabletask.DurableTaskClient

import gov.cdc.dex.csv.BlobServiceMocker
import gov.cdc.dex.csv.ContextMocker
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.EvHubData
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.ActivityParams
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.OrchestratorStep

import java.io.File

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mockito

internal class Unit_FnRouter {
    companion object{
        private val configParentDir = File("src/test/resources/testfiles/config")
        private val ingestParentDir = File("src/test/resources/testfiles")
    }

    private val ranOrchestrations = mutableListOf<OrchestratorInput>()

    @BeforeEach
    internal fun setup(){
        ranOrchestrations.clear()
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //happy path
    @Test
    internal fun happyPath(){
        val event = mockEvent(id="happyPath", url="test-upload.csv")
        val durableContext = mockClientContext()
        val context = ContextMocker.mockExecutionContext()
        val ingestBlobService = BlobServiceMocker.mockBlobService(ingestParentDir, metadata=mapOf("messageType" to "happyPath", "messageVersion" to "DUMMY"))
        val configBlobService = BlobServiceMocker.mockBlobService(configParentDir)
        val baseConfigUrl = ""
        
        val response = FnRouter().pipelineEntry(event, durableContext, context, ingestBlobService, configBlobService, baseConfigUrl)
        Assertions.assertNull(response)
        Assertions.assertEquals(1, ranOrchestrations.size, "wrong number of orchestrations triggered")

        val orchInput = ranOrchestrations.get(0)
        Assertions.assertEquals(OrchestratorInput(
            config=OrchestratorConfiguration(
                steps=listOf(
                    OrchestratorStep(
                        stepNumber="1",
                        functionToRun=FunctionDefinition(functionName="1-functionToRun")
                    )
                ),
                globalErrorFunction=FunctionDefinition(functionName="globalErrorFunction")
            ),
            initialParams=ActivityParams(
                executionId="happyPath",
                originalFileUrl="test-upload.csv"
            )
        ), orchInput)

    }
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //a lot of negative testing
    //- trigger each error message in FnRouter
    //- malform routerConfig (missing fields, extra fields, bad structure, etc)
    //- malform orch config (missing fields, extra fields, bad structure, etc)
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions
    
    private fun mockEvent(id:String?, url:String?, eventType:String? = "Microsoft.Storage.BlobCreated"):AzureBlobCreateEventMessage{
        return AzureBlobCreateEventMessage(eventType, id, EvHubData(url))
    }

    private fun mockClientContext():DurableClientContext{
        val mockContext : DurableClientContext = Mockito.mock(DurableClientContext::class.java)
        Mockito.`when`(mockContext.client).thenAnswer({mockClient()})
        return mockContext
    }

    private fun mockClient():DurableTaskClient{
        val mockClient : DurableTaskClient = Mockito.mock(DurableTaskClient::class.java)
        Mockito.`when`(mockClient.scheduleNewOrchestrationInstance(Mockito.anyString(),Mockito.any(Object::class.java)))
            .thenAnswer({mockOrchestration(it.getArgument(1))})
        return mockClient
    }

    private fun mockOrchestration(input:OrchestratorInput):String{
        ranOrchestrations.add(input)
        return "MOCK_ID"
    }
}