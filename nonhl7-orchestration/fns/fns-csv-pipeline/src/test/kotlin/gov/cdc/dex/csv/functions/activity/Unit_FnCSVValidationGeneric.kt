package gov.cdc.dex.csv.functions.activity

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.models.BlobProperties
import com.azure.storage.blob.specialized.BlobInputStream

import gov.cdc.dex.csv.ContextMocker
import gov.cdc.dex.csv.dtos.ActivityInput
import gov.cdc.dex.csv.dtos.ActivityParams
import gov.cdc.dex.csv.dtos.CommonInput
import gov.cdc.dex.csv.services.IBlobService

import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import kotlin.test.assertEquals

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock



internal class Unit_FnCSVValidationGeneric {
    companion object{
        private val outputParentDir = File("target/test-output/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")))
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //happy path
    @Test
    internal fun happyPath_singleCsv(){
        moveToOutput("test-upload.csv", "happyPath_singleCsv")
        val input = ActivityInput(common=CommonInput("happyPath_singleCsv", ActivityParams(originalFileUrl = "happyPath_singleCsv/test-upload.csv")))
        val response = FnCSVValidationGeneric().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.errorMessage)
        Assertions.assertNull(response.fanOutParams)
    }
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad inputs
    
    @Test
    internal fun negative_url_null(){
        val input = ActivityInput(common=CommonInput("negative_url_null", ActivityParams()))
        val response = FnCSVValidationGeneric().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("No source URL provided!", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }

    @Test
    internal fun negative_url_empty(){
        val input = ActivityInput(common=CommonInput("negative_url_empty", ActivityParams(originalFileUrl = "")))
        val response = FnCSVValidationGeneric().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("No source URL provided!", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }

    @Test
    internal fun negative_url_notFound(){
        val input = ActivityInput(common=CommonInput("negative_url_notFound", ActivityParams(originalFileUrl = "some-other-file.csv")))
        val response = FnCSVValidationGeneric().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("File does not exist!", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad files
    
    @Test
    internal fun negative_file_notCSV(){
        moveToOutput("test-upload.xls","negative_file_notCSV")
        val input = ActivityInput(common=CommonInput("negative_file_unableToZip", ActivityParams(originalFileUrl = "negative_file_unableToZip/test-upload.xls")))
        val response = FnCSVValidationGeneric().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("File is not a .csv!", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions
    
    private fun moveToOutput(fileName:String, testName:String){
        var localFileIn = File("src/test/resources/testfiles",fileName)
        var localFileOut = File(File(outputParentDir,testName),fileName)
        
        localFileOut.parentFile.mkdirs()
        localFileIn.copyTo(localFileOut)
    }

    private fun mockBlobService(contentType:String = "DUMMY_CONTENT"):IBlobService{
        val mockBlobService = Mockito.mock(IBlobService::class.java)

        Mockito.`when`(mockBlobService.exists(Mockito.anyString())).thenAnswer(this::doesTestFileExist)
        Mockito.`when`(mockBlobService.getProperties(Mockito.anyString())).thenAnswer({getBlobProperties(it, contentType)})
        Mockito.`when`(mockBlobService.openDownloadStream(Mockito.anyString())).thenAnswer(this::openInputStream)
        Mockito.`when`(mockBlobService.openUploadStream(Mockito.anyString())).thenAnswer(this::openOutputStream)

        return mockBlobService
    }

    private fun doesTestFileExist(i: InvocationOnMock):Boolean{
        val relativePath:String = i.getArgument(0)
        var localFile = File(outputParentDir,relativePath)
        return localFile.exists()
    }

    private fun getBlobProperties(i: InvocationOnMock, contentType:String):BlobProperties{
        val relativePath:String = i.getArgument(0)

        return BlobProperties(null, null, null, -1, contentType, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
    }

    private fun openInputStream(i: InvocationOnMock):InputStream{
        val relativePath:String = i.getArgument(0)
        var localFile = File(outputParentDir,relativePath)
        return FileInputStream(localFile)
    }

    private fun openOutputStream(i: InvocationOnMock):OutputStream{
        val relativePath:String = i.getArgument(0)
        var localFile = File(outputParentDir,relativePath)
        localFile.parentFile.mkdirs()
        return FileOutputStream(localFile)
    }
}