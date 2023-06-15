package gov.cdc.dex.csv.services

import java.io.InputStream
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.core.type.TypeReference

object JsonParseService{
    private val jsonParser = jacksonObjectMapper()
    
    fun <T> parse(inputStream:InputStream): T {
        return jsonParser.readValue(inputStream, object : TypeReference<T>(){})            
    }
}