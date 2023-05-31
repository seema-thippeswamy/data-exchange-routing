package gov.cdc.dex.csv.dtos

import com.fasterxml.jackson.annotation.JsonCreator


data class OrchestratorInput(
    val config          : OrchestratorConfiguration,
    val initialParams   : Map<String,String>
)

data class OrchestratorConfiguration (
    val steps   : List<OrchestratorStep>
)

data class OrchestratorStep (
    val stepNumber              : String,
    val functionToRun           : FunctionDefinition,
    val customErrorFunction     : FunctionDefinition? = null,
    val fanOutAfter             : Boolean = false,
    val fanInBefore             : Boolean = false
)

data class FunctionDefinition(
    val functionName            : String,
    val functionConfiguration   : Any? = null
)

data class OrchestratorOutput (
    val outputParams    : Map<String,String>,
    val errorMessage    : String? = null,
    val failedStep      : String? = null
)

data class RecursiveOrchestratorInput (
    val input       : OrchestratorInput,
    val enterIndex  : Int
)
data class RecursiveOrchestratorOutput (
    val output      : OrchestratorOutput,
    val leaveIndex  : Int
)