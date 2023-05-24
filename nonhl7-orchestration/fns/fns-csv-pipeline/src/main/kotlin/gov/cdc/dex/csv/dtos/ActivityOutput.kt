package gov.cdc.dex.csv.dtos

data class ActivityOutput (
    val input               : ActivityInput,
    val errorMessage        : String? = null,
    val newParamMap         : Map<String,String>? = null,
    val fanOutNewParamMaps  : List<Map<String,String>>? = null,
)