package gov.cdc.dex.router.dtos

import com.google.gson.annotations.SerializedName

data class RouteConfig(
    @SerializedName("FileType")         val fileType            : String,
    @SerializedName("StagingLocations") val stagingLocations    : StagingLocations
)

data class StagingLocations(
    @SerializedName("DestinationContainer") val destinationContainer    : String,
)