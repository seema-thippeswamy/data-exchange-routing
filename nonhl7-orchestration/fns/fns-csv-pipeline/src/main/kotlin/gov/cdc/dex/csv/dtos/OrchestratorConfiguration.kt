package gov.cdc.dex.csv.dtos

data class OrchestratorConfiguration (
    val steps   : List<OrchestratorStep>
)

data class OrchestratorStep (
    val functionName            : String,
    val functionConfiguration   : Any
)