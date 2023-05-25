package gov.cdc.dex.csv.dtos

data class ActivityInput (
    val stepNumber  : String,
    val config      : Any?,
    val params      : Map<String,String>
)

data class ActivityOutput (
    val newGlobalParams     : Map<String,String>? = null,
    val errorMessage        : String? = null,
    val newFanOutParams     : List<Map<String,String>>? = null,
)

data class FanInActivityInput(
    val stepNumber          : String,
    val config              : Any?,
    val orchestratorOutputs : List<OrchestratorOutput>
)