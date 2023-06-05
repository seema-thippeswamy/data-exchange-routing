package gov.cdc.dex.csv

import com.microsoft.azure.functions.ExecutionContext

import java.util.logging.Logger

import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock;


object ContextMocker {
    fun mockExecutionContext(): ExecutionContext {
        val mockContext : ExecutionContext = Mockito.mock(ExecutionContext::class.java);
        val logger : Logger = Mockito.mock(Logger::class.java);

        Mockito.`when`(mockContext.logger).thenReturn(logger)
        Mockito.`when`(logger.info(Mockito.anyString())).thenAnswer(::loggerInvocation)
        Mockito.`when`(logger.warning(Mockito.anyString())).thenAnswer(::loggerInvocation)
        return mockContext
    }

    private fun loggerInvocation(i: InvocationOnMock){
        val toLog:String = i.getArgument(0);
        println("[log] $toLog");
    }
}