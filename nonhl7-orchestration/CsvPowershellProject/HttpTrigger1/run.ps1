using namespace System.Net

# Input bindings are passed in via param block.
param($Request, $TriggerMetadata)

# Write to the Azure Functions log stream.
Write-Host "PowerShell HTTP trigger function processed a request."

# Interact with query parameters or the body of the request.
$sourceBlobUrl = $Request.Query.sourceBlobUrl
$destinationBlobUrl = $Request.Query.destinationBlobUrl
#$connectionString = $Request.Query.connectionString
if (-not $sourceBlobUrl) {
    $sourceBlobUrl = $Request.Body.sourceBlobUrl

    $body = "This HTTP triggered function executed successfully. Pass a $sourceBlobUrl in the query string or in the request body for a personalized response."
}

if(-not $destinationBlobUrl){
    $destinationBlobUrl = $Request.Body.destinationBlobUrl
    
    $body = "This HTTP triggered function executed successfully. Pass a $destinationBlobUrl in the query string or in the request body for a personalized response."
}

if ($sourceBlobUrl) {

    #$sourceBlobUrl = "https://dexcsvdata001.blob.core.windows.net/testing/input/feed_202001099.csv"
    #$destinationBlobUrl = "https://dexcsvdata001.blob.core.windows.net/testing/output/feed_202001099.csv"
    $connectionString = "DefaultEndpointsProtocol=https;AccountName=dexcsvdata001;AccountKey=fYDnsfGGzpxnr9XvcbzV8t5UPimOtpH3Zs1RM7VLFfCnBxeWTj1p4Epb2Usd8pkFEPXtZp+vUTGq+AStXYIqiw==;EndpointSuffix=core.windows.net"
    $body = "Execution Started "
    # Extract the source container name from the source blob URL
    $sourceContainerName = ($sourceBlobUrl -split '/')[3]

    # Extract the destination container name from the source blob URL
    $destinationContainerName = ($destinationBlobUrl -split '/')[3]

    $spattern = "^.+?/[^/]+/[^/]+/(.+)$"
    $sourceBlobPath = $sourceBlobUrl -replace $spattern, '/$1'
    $sourceBlobPath = $sourceBlobPath -replace "^/", ""

    # Extract the blob name from the destination URL
    $dpattern = "^.+?/[^/]+/[^/]+/(.+)$"
    $destinationBlobPath = $destinationBlobUrl -replace $dpattern, '/$1'
    $destinationBlobPath = $destinationBlobPath -replace "^/", ""
    $body = "Execution second step "
    #Get current date and time

        #$startTime = Get-Date

    #Write-Output "File copy process started at $startTime."

    # Create a new Azure Storage context
    $context = New-AzStorageContext -ConnectionString $connectionString

    # copy the file if the blob exists in the provided path

        #Write-Host "The Blob file "$blobName" exists in the path"
    $copyOperation = Start-AzStorageBlobCopy -SrcContainer $sourceContainerName -SrcBlob $sourceBlobPath -DestContainer $destinationContainerName -DestBlob $destinationBlobPath -Context $context
    Start-Sleep -Seconds 3   
    $copyState = Get-AzStorageBlobCopyState -Context $context -Container $destinationContainerName -Blob $destinationBlobPath
    $body = "Copy operation Started $copyState"
    if ($copyOperation){
        #Wait for the copy operation to complete
        $copyState = Get-AzStorageBlobCopyState -Context $context -Container $destinationContainerName -Blob $destinationBlobPath
        $body = "Current Copy State is $copyState"

        if($copyState.Status -eq "Success"){
            $body = "Copy file is successful on first time"
        }
        elseif($copyState.Status -eq "Pending") {
            while($copyState.Status -eq "Pending"){
                Start-Sleep -Second 5
                $copyState = Wait-AzStorageBlobCopyState -Context $context -Container $destinationContainerName -Blob $destinationBlobPath
                if($copyState.Status -eq "Success"){
                    $body = "Copy file is successful on second time."
                }    
        }
        else{
                $body = "An Error Occured while copying the file."
            }

        }
    }
    else {
        $body = "Copy operation didn't get Initiated $sourceBlobUrl, $destinationBlobUrl, $connectionString"
    }
    }


# Associate values to output bindings by calling 'Push-OutputBinding'.
Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
    StatusCode = [HttpStatusCode]::OK
    Body = $body
})
