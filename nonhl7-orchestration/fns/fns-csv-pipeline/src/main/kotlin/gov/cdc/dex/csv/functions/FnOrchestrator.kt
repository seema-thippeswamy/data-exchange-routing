package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.durabletask.TaskOrchestrationContext
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger

import gov.cdc.dex.csv.dtos.ActivityResponse
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.OrchestratorResponse
import gov.cdc.dex.csv.dtos.OrchestratorStep

class FnOrchestrator {
    @FunctionName("DexCsvOrchestrator")
    fun orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") ctx:TaskOrchestrationContext
    ):OrchestratorResponse {
        val configuration = ctx.getInput(OrchestratorConfiguration::class.java)

        for(step in configuration.steps){
            runStep(ctx,step)
        }

        //TODO build response
        return OrchestratorResponse(configuration)
    }

    private fun runStep(ctx:TaskOrchestrationContext,step:OrchestratorStep){
        try{
            //TODO handle fan-out and fan-in
            var functionResponse = ctx.callActivity(step.functionName, step.functionConfiguration, ActivityResponse::class.java)
            //TODO something with the response
        }catch(e:Exception){
            //TODO error handling based on how the step is configured
        }
    }
}
