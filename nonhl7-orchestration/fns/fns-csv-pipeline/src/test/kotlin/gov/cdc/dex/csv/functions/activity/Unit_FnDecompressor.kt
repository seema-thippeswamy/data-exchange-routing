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



internal class Unit_FnDecompressor {
    companion object{
        private val parentDir = File("target/test-output/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")))
        private val ingestParentDir = File(parentDir, "ingest")
        private val processedParentDir = File(parentDir, "processed")
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //happy path
    @Test
    internal fun happyPath_singleCsv(){
        val inputUrl = moveToIngest("test-upload.csv", "happyPath_singleCsv")
        val input = ActivityInput(common=CommonInput("happyPath_singleCsv", ActivityParams(originalFileUrl = inputUrl)))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.errorMessage)

        val fanOut = response.fanOutParams
        Assertions.assertNotNull(fanOut)
        Assertions.assertEquals(1, fanOut!!.size, "Invalid number of fan out files")
        Assertions.assertEquals(ActivityParams(originalFileUrl = "happyPath_singleCsv/test-upload.csv", currentFileUrl = "happyPath_singleCsv/test-upload.csv"), fanOut[0])
        Assertions.assertTrue(File(processedParentDir, "happyPath_singleCsv/test-upload.csv").exists(), "file not in processed!")
    }
    
    @Test
    internal fun happyPath_zip(){
        val inputUrl = moveToIngest("test-upload-zip.zip","happyPath_zip")
        val input = ActivityInput(common=CommonInput("happyPath_zip", ActivityParams(originalFileUrl = inputUrl)))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService("application/zip"))
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.errorMessage)

        val fanOut = response.fanOutParams
        Assertions.assertNotNull(fanOut)
        Assertions.assertEquals(4, fanOut!!.size, "Invalid number of fan out files")
        System.out.println(fanOut)
        
        val okFileNameList = mutableListOf(
            "happyPath_zip/test-upload-zip/test-upload-1.csv",
            "happyPath_zip/test-upload-zip/test-upload-2.csv",
            "happyPath_zip/test-upload-zip/test-upload-3/test-upload.csv",
            "happyPath_zip/test-upload-zip/test-upload-inner/test-upload-4.csv"
        )

        for(param in fanOut){
            Assertions.assertEquals("happyPath_zip/test-upload-zip.zip", param.originalFileUrl)
            val okFileName = param.currentFileUrl
            Assertions.assertNotNull(okFileName)
            Assertions.assertTrue(File(processedParentDir, okFileName).exists(), "file not in processed! $okFileName")
            Assertions.assertTrue(okFileNameList.remove(okFileName), "bad processed URL, potentially a duplicate")
        }

        Assertions.assertTrue(okFileNameList.isEmpty(), "some URLs not in messages $okFileNameList")
    }

    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad inputs
    
    @Test
    internal fun negative_url_null(){
        val input = ActivityInput(common=CommonInput("negative_url_null", ActivityParams()))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("No source URL provided!", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }

    @Test
    internal fun negative_url_empty(){
        val input = ActivityInput(common=CommonInput("negative_url_empty", ActivityParams(originalFileUrl = "")))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("No source URL provided!", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }

    @Test
    internal fun negative_url_notFound(){
        val input = ActivityInput(common=CommonInput("negative_url_notFound", ActivityParams(originalFileUrl = "some-other-file.csv")))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService())
        Assertions.assertEquals("File missing in Azure! some-other-file.csv", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad files
    
    @Test
    internal fun negative_file_unableToZip(){
        val inputUrl = moveToIngest("test-upload.csv","negative_file_unableToZip")
        val input = ActivityInput(common=CommonInput("negative_file_unableToZip", ActivityParams(originalFileUrl = inputUrl)))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService("application/zip"))
        Assertions.assertEquals("Zipped file was empty: negative_file_unableToZip/test-upload.csv", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }
    
    @Test
    internal fun negative_file_emptyZip(){
        val inputUrl = moveToIngest("test-empty.zip","negative_file_emptyZip")
        val input = ActivityInput(common=CommonInput("negative_file_emptyZip", ActivityParams(originalFileUrl = inputUrl)))
        val response = FnDecompressor().process(input, ContextMocker.mockExecutionContext(), mockBlobService("application/zip"))
        Assertions.assertEquals("Zipped file was empty: negative_file_emptyZip/test-empty.zip", response.errorMessage)
        Assertions.assertNull(response.updatedParams)
        Assertions.assertNull(response.fanOutParams)
    }
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions
    
    private fun moveToIngest(fileName:String, testName:String):String{
        var localFileIn = File("src/test/resources/testfiles",fileName)
        var localFileOut = File(File(ingestParentDir,testName),fileName)
        
        localFileOut.parentFile.mkdirs()
        localFileIn.copyTo(localFileOut)

        return "ingest/$testName/$fileName"
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
        var localFile = File(parentDir,relativePath)
        return localFile.exists()
    }

    private fun getBlobProperties(i: InvocationOnMock, contentType:String):BlobProperties{
        val relativePath:String = i.getArgument(0)

        return BlobProperties(null, null, null, -1, contentType, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
    }

    private fun openInputStream(i: InvocationOnMock):InputStream{
        val relativePath:String = i.getArgument(0)
        var localFile = File(parentDir,relativePath)
        return FileInputStream(localFile)
    }

    private fun openOutputStream(i: InvocationOnMock):OutputStream{
        val relativePath:String = i.getArgument(0)
        var localFile = File(parentDir,relativePath)
        localFile.parentFile.mkdirs()
        return FileOutputStream(localFile)
    }
}