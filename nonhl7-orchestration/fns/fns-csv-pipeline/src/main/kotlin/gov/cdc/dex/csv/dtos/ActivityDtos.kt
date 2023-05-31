package gov.cdc.dex.csv.dtos

data class ActivityInput (
    val stepNumber      : String,
    val config          : Any?,
    val globalParams    : Map<String,String>,
    val fanInParams     : List<Map<String,String>>? = null
)

data class ActivityOutput (
    val newGlobalParams     : Map<String,String> = mapOf(),
    val errorMessage        : String? = null,
    val newFanOutParams     : List<Map<String,String>>? = null
)