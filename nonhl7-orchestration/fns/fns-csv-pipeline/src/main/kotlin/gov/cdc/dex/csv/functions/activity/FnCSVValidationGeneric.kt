package gov.cdc.dex.csv.functions.activity

import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

import gov.cdc.dex.csv.dtos.ActivityInput
import gov.cdc.dex.csv.dtos.ActivityOutput
import gov.cdc.dex.csv.services.IBlobService
import gov.cdc.dex.csv.services.AzureBlobServiceImpl
import gov.cdc.dex.csv.constants.EnvironmentParams

import java.util.logging.Level

class FnCSVValidationGenericEntry{
    @FunctionName("FnCSVValidationGeneric")
    fun process(
        @DurableActivityTrigger(name = "input") input: ActivityInput, 
        context: ExecutionContext 
    ):ActivityOutput{
        val blobConnectionString = System.getenv(EnvironmentParams.INGEST_BLOB_CONNECTION_PARAM) 
        if(blobConnectionString == null){
            throw IllegalArgumentException("${EnvironmentParams.INGEST_BLOB_CONNECTION_PARAM} Environment variable not defined")
        }
        val blobService = AzureBlobServiceImpl(blobConnectionString)

        return FnCSVValidationGeneric().process(input, context, blobService)
    }
}

class FnCSVValidationGeneric {

    fun process(input: ActivityInput, context: ExecutionContext, blobService:IBlobService): ActivityOutput { 
                
        context.getLogger().info("Running CSV Validator (Generic) for input $input");

        val sourceUrl = input.common.params.originalFileUrl

        if(sourceUrl.isNullOrBlank()){
            return ActivityOutput(errorMessage = "No source URL provided!")
        }

        if(!sourceUrl.endsWith(".csv")){
            return ActivityOutput(errorMessage = "File is not a .csv!")
        }

        if(!blobService.exists(sourceUrl)){
            return ActivityOutput(errorMessage = "File does not exist!")
        }
        
        return ActivityOutput()
    }
}