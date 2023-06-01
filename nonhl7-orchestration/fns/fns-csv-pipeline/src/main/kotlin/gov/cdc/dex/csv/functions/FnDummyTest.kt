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
import gov.cdc.dex.csv.dtos.CommonInput
import gov.cdc.dex.csv.dtos.ActivityParams

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

        val config = OrchestratorConfiguration(steps, FunctionDefinition("DummyActivity", mapOf("configKey" to "configValueError")))
        val initialParams = ActivityParams("originalFileLocation")
        return OrchestratorInput(config,initialParams)
    }

    @FunctionName("DummyActivity")
    fun runActivity(@DurableActivityTrigger(name = "input") input:DummyInput,  context:ExecutionContext):ActivityOutput {
       // throw RuntimeException("BLAH")
        context.getLogger().info("Running dummy activity for input $input");
        input.common.params.originalFileLocation = input.config.configKey
        return ActivityOutput(input.common.params);
    }
}


data class DummyInput(
    val config  : DummyConfig,
    val common  : CommonInput
)

data class DummyConfig(
    val configKey : String
)
