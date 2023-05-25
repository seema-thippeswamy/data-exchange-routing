package gov.cdc.dex.csv.dtos

data class OrchestratorInput(
    val config          : OrchestratorConfiguration,
    val initialParams   : Map<String,String>
)

data class OrchestratorOutput (
    val outputParams    : Map<String,String>,
    val errorMessage    : String? = null,
    val failedStep      : String? = null
)

data class OrchestratorConfiguration (
    val steps   : List<OrchestratorStep>
)

data class OrchestratorStep (
    val stepNumber              : String,
    val functionToRun           : FunctionDefinition,
    val customErrorFunction     : FunctionDefinition?,
    val fanOutAfter             : Boolean? = null,
    val fanInBefore             : Boolean? = null
)

data class FunctionDefinition(
    val functionName            : String,
    val functionConfiguration   : Any? = null
)