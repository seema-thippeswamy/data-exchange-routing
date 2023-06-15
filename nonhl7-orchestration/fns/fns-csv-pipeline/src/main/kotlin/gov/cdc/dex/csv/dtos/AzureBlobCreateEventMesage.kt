package gov.cdc.dex.csv.dtos

import com.google.gson.annotations.SerializedName

//NOTE: there are more fields in the message, but these are the only ones we care about
data class AzureBlobCreateEventMessage (
    val eventType : String?,
    val id        : String?,
    val evHubData : EvHubData?
)

data class EvHubData (
    val url       : String?
)