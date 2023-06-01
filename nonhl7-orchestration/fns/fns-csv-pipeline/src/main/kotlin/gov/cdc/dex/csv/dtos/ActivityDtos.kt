package gov.cdc.dex.csv.dtos

data class ActivityInput (
    val config  : Any?,
    val common  : CommonInput
)

data class CommonInput (
    val stepNumber  : String,
    val params      : ActivityParams,
    val fanInParams : List<ActivityParams>? = null
)

data class ActivityOutput (
    val updatedParams   : ActivityParams,
    val errorMessage    : String? = null,
    val fanOutParams    : List<ActivityParams>? = null
)

data class ActivityParams (
    var originalFileLocation    : String? = null,
    var currentFileLocation     : String? = null,
    var errorMessage            : String? = null
)