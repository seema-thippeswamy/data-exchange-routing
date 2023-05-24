package gov.cdc.dex.csv.dtos

data class ActivityInput (
    val stepNumber  : Int,
    val config      : Map<String,String>,
    val paramMap    : Map<String,String>
)