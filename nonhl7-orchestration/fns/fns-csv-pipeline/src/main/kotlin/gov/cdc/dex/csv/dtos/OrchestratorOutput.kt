package gov.cdc.dex.csv.dtos

data class OrchestratorOutput (
    val input           : OrchestratorInput,
    val errorMessage    : String? = null,
    val failedStep      : Int? = null
)