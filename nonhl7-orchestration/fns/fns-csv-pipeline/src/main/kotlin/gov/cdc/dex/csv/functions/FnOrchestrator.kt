package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.durabletask.Task
import com.microsoft.durabletask.TaskOrchestrationContext
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger

import gov.cdc.dex.csv.dtos.ActivityInput
import gov.cdc.dex.csv.dtos.ActivityOutput
import gov.cdc.dex.csv.dtos.FanInActivityInput
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorOutput
import gov.cdc.dex.csv.dtos.OrchestratorStep
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration

class FnOrchestrator {
    companion object{
        private val defaultErrorName = "DexCsvDefaultError"
        private val defaultFanInName = "DexCsvDefaultFanIn"
    }

    @FunctionName("DexCsvOrchestrator")
    fun orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") ctx:TaskOrchestrationContext
    ) : OrchestratorOutput{
        val input = ctx.getInput(OrchestratorInput::class.java)
        val configuration = input.config

        val globalParamMap = input.initialParams.toMutableMap()

        for(step in configuration.steps){
            val stepInput = ActivityInput(step.stepNumber, step.functionToRun.functionConfiguration, globalParamMap)
            val stepOutput = runActivity(ctx, step.functionToRun.functionName, stepInput)
            
            val errorMessage = if(stepOutput.errorMessage!=null){
                stepOutput.errorMessage
            }else if(step.fanOutSteps != null && stepOutput.newFanOutParams == null){
                "Tried to fan out but function did not return correct parameters"
            }else{
                null
            }

            if(errorMessage != null){
                runError(ctx, step, globalParamMap, errorMessage)
                return OrchestratorOutput(globalParamMap, errorMessage, step.stepNumber)
            }
    
            val newGlobalParams = if(step.fanOutSteps !=null){
                //fanned out params being null was already checked in the error handling

                val parallelTasks:MutableList<Task<OrchestratorOutput>> = mutableListOf()
                for(fannedOutParamMap in stepOutput.newFanOutParams!!){
                    val subMap = mutableMapOf<String,String>()
                    subMap.putAll(globalParamMap)
                    subMap.putAll(fannedOutParamMap)
                    val subInput = OrchestratorInput(OrchestratorConfiguration(step.fanOutSteps), subMap)
                    parallelTasks.add(ctx.callSubOrchestrator("DexCsvOrchestrator", subInput, OrchestratorOutput::class.java))
                }
                val subOutputs = ctx.allOf(parallelTasks).await()

                val fanInFunction = step.customFanInFunction ?: FunctionDefinition(defaultFanInName)
                val fanInInput = FanInActivityInput(step.stepNumber, fanInFunction.functionConfiguration, subOutputs)
                val fanInOutput = runActivity(ctx, fanInFunction.functionName, fanInInput)

                if(fanInOutput.errorMessage != null){
                    runError(ctx, step, globalParamMap, fanInOutput.errorMessage)
                    return OrchestratorOutput(globalParamMap, errorMessage, step.stepNumber)
                }
                
                fanInOutput.newGlobalParams
            }else{
                stepOutput.newGlobalParams
            }

            if(newGlobalParams != null){
                globalParamMap.putAll(newGlobalParams)
            }
        }
        return OrchestratorOutput(outputParams=globalParamMap)
    }

    private fun runActivity(ctx:TaskOrchestrationContext, functionName:String, activityInput:Any)
    :ActivityOutput{
        val activityOutput = try{ 
            ctx.callActivity(functionName, activityInput, ActivityOutput::class.java).await()
        }catch(e:Exception){
            //TODO somehow log the exception, still need to figure this out
            ActivityOutput(errorMessage = e.localizedMessage, newGlobalParams = mapOf())
        }
        return activityOutput;
    }

    private fun runError(ctx:TaskOrchestrationContext, step:OrchestratorStep, globalParamMap:MutableMap<String,String>, errorMessage:String){
        globalParamMap.put("errorMessage", errorMessage)
        val errorFunction = step.customErrorFunction ?: FunctionDefinition(defaultErrorName)
        val errorInput = ActivityInput(step.stepNumber, errorFunction.functionConfiguration, globalParamMap)
        runActivity(ctx, errorFunction.functionName, errorInput)
    }
}
