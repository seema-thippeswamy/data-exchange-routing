package gov.cdc.dex.csv.constants

enum class EnvironmentParam(val paramKey:String){
    INGEST_BLOB_CONNECTION("BlobConnection"),
    CONFIG_BLOB_CONNECTION("BlobConnection"),
    BASE_CONFIG_URL("BaseConfigUrl");

    fun getSystemValue():String{
        val value = System.getenv(paramKey) 
        if(value == null){
            throw IllegalArgumentException("$paramKey Environment variable not defined")
        }
        return value
    }
}