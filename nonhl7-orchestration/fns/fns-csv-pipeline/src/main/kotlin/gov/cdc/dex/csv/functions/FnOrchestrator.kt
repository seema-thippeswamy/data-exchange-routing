package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.durabletask.Task
import com.microsoft.durabletask.TaskOrchestrationContext
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger

import gov.cdc.dex.csv.dtos.ActivityInput
import gov.cdc.dex.csv.dtos.ActivityOutput
import gov.cdc.dex.csv.dtos.FunctionDefinition
import gov.cdc.dex.csv.dtos.OrchestratorInput
import gov.cdc.dex.csv.dtos.OrchestratorOutput
import gov.cdc.dex.csv.dtos.OrchestratorStep
import gov.cdc.dex.csv.dtos.OrchestratorConfiguration
import gov.cdc.dex.csv.dtos.RecursiveOrchestratorOutput
import gov.cdc.dex.csv.dtos.RecursiveOrchestratorInput
import gov.cdc.dex.csv.dtos.CommonInput
import gov.cdc.dex.csv.dtos.ActivityParams
import java.util.logging.Level

class FnOrchestrator {
    companion object{
        private val defaultErrorName = "DexCsvDefaultError"
    }

    @FunctionName("DexCsvOrchestrator")
    fun orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") taskContext:TaskOrchestrationContext,
        functionContext:ExecutionContext
    ) : OrchestratorOutput{

        //get input passed in from client function
        val input = try{
            taskContext.getInput(OrchestratorInput::class.java)
        }catch(e:Exception){
            log(Level.SEVERE, "Could not retrieve orchestrator input", e, taskContext, functionContext)
            throw e
        }
        log(level=Level.INFO, msg="Started Orchestration with params $input", taskContext=taskContext, functionContext=functionContext)

        //create input for initial recursive call
        val subInput = RecursiveOrchestratorInput(input, 0)
        val recursiveOutput = taskContext.callSubOrchestrator("DexCsvOrchestrator_Recursive", subInput, RecursiveOrchestratorOutput::class.java).await()

        //check overall execution
        val (errorMessage, failedStep) = if(recursiveOutput.output.errorMessage != null){
            Pair(recursiveOutput.output.errorMessage, recursiveOutput.output.failedStep)
        }else if(recursiveOutput.leaveIndex != input.config.steps.size){
            Pair("Orchestrator did not run the correct number of steps! Most likely an issue with the fan-out/fan-in specification", recursiveOutput.leaveIndex.toString())
        }else{
            Pair(null,null)
        }

        if(errorMessage != null){
            val errorInput = ActivityInput(input.config.globalErrorFunction.functionConfiguration, CommonInput("-1", recursiveOutput.output.outputParams))
            runActivity(taskContext, functionContext, input.config.globalErrorFunction.functionName, errorInput)
            return OrchestratorOutput(recursiveOutput.output.outputParams, errorMessage, failedStep)
        }
        
        return OrchestratorOutput(outputParams=recursiveOutput.output.outputParams)
    }

    @FunctionName("DexCsvOrchestrator_Recursive")
    fun recursiveOrchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") taskContext:TaskOrchestrationContext,
        functionContext:ExecutionContext
    ) : RecursiveOrchestratorOutput{
        val recursiveInput = taskContext.getInput(RecursiveOrchestratorInput::class.java)

        val steps = recursiveInput.input.config.steps
        var globalParams = recursiveInput.input.initialParams

        var indexToRun = recursiveInput.enterIndex

        var paramsToFanIn:List<ActivityParams>? = null

        while(indexToRun < steps.size){
            val step = steps[indexToRun]

            val stepInput = if(step.fanInBefore && paramsToFanIn == null){
                //we need to return to the prior level before running this step
                return RecursiveOrchestratorOutput(OrchestratorOutput(globalParams), indexToRun)
            }else if(step.fanInBefore){
                val fanInInput = ActivityInput(step.functionToRun.functionConfiguration, CommonInput(step.stepNumber, globalParams, paramsToFanIn))
                paramsToFanIn = null
                fanInInput
            }else{
                ActivityInput(step.functionToRun.functionConfiguration, CommonInput(step.stepNumber, globalParams))
            }

            //run the step
            val stepOutput = runActivity(taskContext, functionContext, step.functionToRun.functionName, stepInput)

            //check for errors
            val errorMessage = if(stepOutput.errorMessage!=null){
                stepOutput.errorMessage
            }else if(step.fanOutAfter && stepOutput.fanOutParams == null){
                "Tried to fan out but function did not return correct parameters"
            }else{
                null
            }
            if(errorMessage != null){
                runError(taskContext, functionContext, step, globalParams, errorMessage)
                return RecursiveOrchestratorOutput(OrchestratorOutput(globalParams, errorMessage, step.stepNumber),indexToRun)
            }

            //if global parameters are updated, replace
            globalParams = stepOutput.updatedParams ?: globalParams

            //update indexToRun based on whether fanning out or not
            indexToRun = if(!step.fanOutAfter){
                indexToRun + 1
            }else{
                val parallelTasks:MutableList<Task<RecursiveOrchestratorOutput>> = mutableListOf()
                //fanned out params being null was already checked in the error handling
                for(fannedOutParams in stepOutput.fanOutParams!!){
                    val subInput = RecursiveOrchestratorInput(OrchestratorInput(recursiveInput.input.config, fannedOutParams),(indexToRun+1))
                    parallelTasks.add(taskContext.callSubOrchestrator("DexCsvOrchestrator_Recursive", subInput, RecursiveOrchestratorOutput::class.java))
                }

                val subOutputs = taskContext.allOf(parallelTasks).await()

                val fanInIndex = subOutputs[0].leaveIndex
                val fanInErrorMessage = if(subOutputs.any{it.output.errorMessage != null}){
                    var errMsg = "Fanned-out branches failed: "
                    errMsg += subOutputs.filter{it.output.errorMessage != null}.joinToString()
                    errMsg
                }else if(subOutputs.any{it.leaveIndex != fanInIndex}){
                    "Mismatched fan-in indices"+subOutputs.joinToString() { it.leaveIndex.toString() }
                }else{
                    null
                }

                if(fanInErrorMessage!=null){
                    return RecursiveOrchestratorOutput(OrchestratorOutput(globalParams, fanInErrorMessage, "Fanned-out starting from ${step.stepNumber}"),fanInIndex)
                }

                //set parameters so the code knows that we are fanning in here, and not returning
                paramsToFanIn = subOutputs.asSequence().map{it.output.outputParams}.toList()

                //next to be ran is the fan in
                fanInIndex
            }
        }
        return RecursiveOrchestratorOutput(OrchestratorOutput(outputParams=globalParams),indexToRun)

    
    }

    private fun runActivity(
        taskContext:TaskOrchestrationContext, functionContext:ExecutionContext, functionName:String, activityInput:ActivityInput
    ):ActivityOutput{
        val activityOutput = try{ 
            taskContext.callActivity(functionName, activityInput, ActivityOutput::class.java).await()
        }catch(e:com.microsoft.durabletask.TaskFailedException){
            log(Level.SEVERE, "Error in step ${activityInput.common.stepNumber}", e, taskContext, functionContext)
            ActivityOutput(errorMessage = e.localizedMessage, updatedParams = activityInput.common.params)
        }
        return activityOutput;
    }

    private fun runError(
        taskContext:TaskOrchestrationContext, functionContext:ExecutionContext, step:OrchestratorStep, globalParams:ActivityParams, errorMessage:String
    ){
        globalParams.errorMessage = errorMessage
        val errorFunction = step.customErrorFunction ?: FunctionDefinition(defaultErrorName)
        val errorInput = ActivityInput(errorFunction.functionConfiguration, CommonInput(step.stepNumber, globalParams))
        runActivity(taskContext, functionContext, errorFunction.functionName, errorInput)
    }

    private fun log(
        level:Level, msg:String, thrown:Throwable?=null, taskContext:TaskOrchestrationContext, functionContext:ExecutionContext
    ){
        if (!taskContext.getIsReplaying()) {
            if(thrown == null){
                functionContext.getLogger().log(level, msg);
            } else{
                functionContext.getLogger().log(level, msg, thrown);
            }
        }
    }
}
