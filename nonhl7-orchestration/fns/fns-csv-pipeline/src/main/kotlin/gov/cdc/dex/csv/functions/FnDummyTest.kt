package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.HttpTrigger
import com.microsoft.azure.functions.annotation.AuthorizationLevel
import com.microsoft.azure.functions.HttpMethod
import com.microsoft.azure.functions.HttpRequestMessage
import com.microsoft.azure.functions.HttpResponseMessage
import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.durabletask.azurefunctions.DurableClientInput
import com.microsoft.durabletask.azurefunctions.DurableClientContext
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger
import com.microsoft.durabletask.DurableTaskClient

import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.OrchestratorStep
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.ActivityOutput

import java.util.Optional

class FnDummyTest {
    @FunctionName("DummyStartOrchestration")
    fun startOrchestration(
            @HttpTrigger(name = "req", methods = [HttpMethod.GET, HttpMethod.POST], authLevel = AuthorizationLevel.ANONYMOUS) request:HttpRequestMessage<String?>,
            @DurableClientInput(name = "durableContext") durableContext:DurableClientContext,
            context:ExecutionContext):HttpResponseMessage {
        context.getLogger().info("Java HTTP trigger processed a request.");

        val client = durableContext.getClient();
        val instanceId = client.scheduleNewOrchestrationInstance("DexCsvOrchestrator", buildOrchestratorInput());
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    private fun buildOrchestratorInput():OrchestratorInput{
        val steps = mutableListOf<OrchestratorStep>()
        steps.add(OrchestratorStep("1", FunctionDefinition("DummyActivity", mapOf("configKey" to "configValue1"))))
        steps.add(OrchestratorStep("2", FunctionDefinition("DummyActivity", mapOf("configKey" to "configValue2"))))

        val config = OrchestratorConfiguration(steps)
        val initialParams = mapOf("dummyKey1" to "dummyValue1","dummyKey2" to "dummyValue2")
        return OrchestratorInput(config,initialParams)
    }

    @FunctionName("DummyActivity")
    fun runActivity(@DurableActivityTrigger(name = "input") input:DummyInput,  context:ExecutionContext):ActivityOutput {
       // throw RuntimeException("BLAH")
        context.getLogger().info("Running dummy activity for input $input");
        return ActivityOutput(mapOf(input.config.configKey to input.params.dummyKey1));
    }
}


data class DummyInput(
    val stepNumber  : String,
    val config      : DummyConfig,
    val params      : DummyParams
)

data class DummyConfig(
    val configKey : String
)

data class DummyParams(
    val dummyKey1 : String,
    val dummyKey3 : String
)