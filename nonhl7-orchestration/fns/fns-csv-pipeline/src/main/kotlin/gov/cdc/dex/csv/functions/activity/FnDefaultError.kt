package gov.cdc.dex.csv.functions.activity

import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

import gov.cdc.dex.csv.dtos.ActivityInput

class FnDefaultError {
    @FunctionName("DexCsvDefaultError")
    fun defaultError(
        @DurableActivityTrigger(name = "input") input: ActivityInput, 
        context: ExecutionContext 
    ){
        context.logger.severe("Pipeline failed on step ${input.stepNumber}, with error message [${input.paramMap.get("errorMessage")}]")
    }