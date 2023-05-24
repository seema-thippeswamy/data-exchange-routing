package gov.cdc.dex.csv.dtos

data class OrchestratorInput(
    val config      : OrchestratorConfiguration,
    val initialParamMap    : Map<String,String>
)

data class OrchestratorConfiguration (
    val steps   : List<OrchestratorStep>
)

data class OrchestratorStep (
    val stepNumber              : Int,
    val functionToRun           : FunctionDefinition,
    val customErrorFunction     : FunctionDefinition?,
    val fanOutSteps             : List<OrchestratorStep>?,
    val fanInFunction           : FunctionDefinition?
)

data class FunctionDefinition(
    val functionName            : String,
    val functionConfiguration   : Map<String,String>
)