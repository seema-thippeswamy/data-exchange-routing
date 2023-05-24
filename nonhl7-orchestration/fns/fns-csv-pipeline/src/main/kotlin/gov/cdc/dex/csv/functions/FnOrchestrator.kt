package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.durabletask.Task
import com.microsoft.durabletask.TaskOrchestrationContext
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger

import gov.cdc.dex.csv.dtos.ActivityInput
import gov.cdc.dex.csv.dtos.ActivityOutput
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorOutput
import gov.cdc.dex.csv.dtos.OrchestratorStep
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration

class FnOrchestrator {
    companion object{
        private val defaultErrorName = "DexCsvDefaultError"
    }

    @FunctionName("DexCsvOrchestrator")
    fun orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") ctx:TaskOrchestrationContext
    ) : OrchestratorOutput{
        val input = ctx.getInput(OrchestratorInput::class.java)
        val configuration = input.config

        var nextParamMap = input.initialParamMap

        for(step in configuration.steps){
            val stepOutput = runStep(ctx, step, nextParamMap)

            val errorMessage = handleErrors(ctx, step, stepOutput)
            if(errorMessage != null){
                return OrchestratorOutput(input, errorMessage, step.stepNumber)
            }
    
            if(step.fanOutSteps !=null){
                //fanned out params being null was already checked in the error handling

                val parallelTasks:MutableList<Task<OrchestratorOutput>> = mutableListOf()
                for(fannedOutParamMap in stepOutput.fanOutNewParamMaps!!){
                    val subInput = OrchestratorInput(OrchestratorConfiguration(step.fanOutSteps), fannedOutParamMap)
                    parallelTasks.add(ctx.callSubOrchestrator("DexCsvOrchestrator",subInput, OrchestratorOutput::class.java))
                }
                val subOutputs = ctx.allOf(parallelTasks).await()

                //TODO fan in
                //if not fan in, check to make sure no more steps are defined
            }else{
                //params being null was already checked in the error handling
                nextParamMap = stepOutput.newParamMap!!
            }

        }
        return OrchestratorOutput(input=input)
    }

    private fun runStep(ctx:TaskOrchestrationContext, step:OrchestratorStep, paramMap:Map<String,String>):ActivityOutput{
        val stepInput = ActivityInput(step.stepNumber,step.functionToRun.functionConfiguration, paramMap)
        val stepOutput = try{ 
            //TODO handle fan-out and fan-in
            val stepOutput = ctx.callActivity(step.functionToRun.functionName, stepInput, ActivityOutput::class.java).await()

            stepOutput
        }catch(e:Exception){
            //TODO somehow log the exception, still need to figure this out
            ActivityOutput(stepInput, e.localizedMessage)
        }
        return stepOutput;
    }

    private fun handleErrors(ctx:TaskOrchestrationContext, step:OrchestratorStep, stepOutput:ActivityOutput):String?{
        val errorMessage = if(stepOutput.errorMessage!=null){
            stepOutput.errorMessage
        }else if(step.fanOutSteps != null && stepOutput.fanOutNewParamMaps == null){
            "Tried to fan out but function did not return correct parameters"
        }else if(step.fanOutSteps == null && stepOutput.newParamMap == null){
            "Function did not return parameters for next step in pipeline"
        }else{
            null
        }

        if(errorMessage != null){
            val errorFunction = step.customErrorFunction ?: FunctionDefinition(defaultErrorName,mapOf())
            val errorParamMap = stepOutput.input.paramMap.toMutableMap()
            errorParamMap.put("errorMessage", errorMessage)
            val errorInput = ActivityInput(step.stepNumber, errorFunction.functionConfiguration, errorParamMap)
            
            ctx.callActivity(errorFunction.functionName, errorInput).await()
        }
        return errorMessage
    }
}
