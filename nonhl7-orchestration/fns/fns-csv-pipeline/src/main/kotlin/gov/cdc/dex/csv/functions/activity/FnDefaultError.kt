package gov.cdc.dex.csv.functions.activity

import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

import gov.cdc.dex.csv.dtos.ActivityOutput

class FnDefaultError {
    @FunctionName("DexCsvDefaultError")
    fun defaultError(
        @DurableActivityTrigger(name = "input") input: DefaultErrorInput, 
        context: ExecutionContext 
    ):ActivityOutput{
        context.logger.severe("Pipeline failed on step ${input.stepNumber}, with error message [${input.params.errorMessage}]")
        return ActivityOutput()//TODO do we need to do anything with this?
    }
}

data class DefaultErrorInput(
    val stepNumber  : String,
    val params      : DefaultErrorParams
)

//currently no config, include if needed

data class DefaultErrorParams(
    val errorMessage    : String
)