package gov.cdc.dex.csv.functions.activity

import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

import gov.cdc.dex.csv.dtos.ActivityOutput
import gov.cdc.dex.csv.dtos.FanInActivityInput
import gov.cdc.dex.csv.dtos.OrchestratorOutput

class FnDefaultFanIn {
    @FunctionName("DexCsvDefaultFanIn")
    fun defaultError(
        @DurableActivityTrigger(name = "input") input: FanInActivityInput, 
        context: ExecutionContext 
    ):ActivityOutput{

        var errorMessage = ""
        val groupedParamMap = mutableMapOf<String,MutableSet<String>>()

        for(output in input.orchestratorOutputs){
            if(output.errorMessage!=null){
                errorMessage += "\n -${output.errorMessage}"
            }

            //if errors have already been found, don't worry about any more map parsing
            if(errorMessage.isEmpty()){
                val branchMap = output.outputParams
                for(key in branchMap.keys){
                    val valueSet = groupedParamMap.getOrDefault(key, mutableSetOf())
                    valueSet.add(branchMap.getValue(key))
                    groupedParamMap.put(key, valueSet)
                }
            }
        }

        val output = if(errorMessage.isEmpty()){
            val newGlobalParams = mutableMapOf<String,String>()
            for(key in groupedParamMap.keys){
                val valueSet = groupedParamMap.getValue(key)
                val toSet = if(valueSet.size==1){
                    valueSet.first()
                }else{
                    valueSet.joinToString(separator = "|")
                }
                newGlobalParams.put(key, toSet)
            }

            ActivityOutput(newGlobalParams = newGlobalParams)
        }else{
            ActivityOutput(errorMessage = errorMessage)
        }

        return output
    }
}
