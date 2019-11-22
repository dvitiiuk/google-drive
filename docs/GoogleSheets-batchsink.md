# Google Drive Batch Source


Description
-----------
Reads spreadsheets from specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the source folder.

**Spreadsheet Name Field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set Google API will use the value of **Default Spreadsheet name** property.

**Default Spreadsheet Name:** Default spreadsheet file name. 
Is used when user don't specify schema field as spreadsheet name.

**Sheet Name Field:** Name of the schema field (should be STRING type) which will be used as sheet title. 
Is optional. In the case it is not set Google API will use the value of **Default sheet name** property.

**Default Sheet Name:** Default sheet title. Is used when user don't specify schema field as sheet title.

**Write Schema As First Row:** Toggle that defines should the sink write out the input schema as first row of an 
out sheet.

### Authentication

**Authentication Type:** Type of authentication used to access Google API. 
OAuth2 and Service account types are available.

#### OAuth2 Properties

**Client ID:** OAuth2 client id.

**Client Secret:** OAuth2 client secret.

**Refresh Token:** OAuth2 refresh token.

**Access Token:** OAuth2 access token.

#### Service Account Properties

**Account File Path:** Path on the local file system of the service account key used for authorization. 
Can be set to 'auto-detect' for getting service account from system variable.
The file/system variable must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

### Retrying

**Max Retry Count:** Maximum number of retry attempts.

**Max Retry Wait:** Maximum wait time for attempt in seconds. Wait time starts from one second and grows exponentially.

**Max Retry Jitter Wait:** Maximum additional wait time in milliseconds.

### Buffering And Paralellization

**Threads Number:** Number of threads which send batched API requests. 
The greater value allows to process records quickly, but requires extended Google Sheets API quota.

**Maximal Buffer Size:** Maximal size in records of the batch API request. 
The greater value allows to reduce the number of API requests, but causes growing of their size.

**Records Queue Length:** Size of the queue used to receive records and for onwards grouping of them to 
batched API requests. For the greater value there is more likely that the sink will group arrived records in the 
butches of maximal size. Also greater value means more memory consumption.

**Maximum Flush Interval:** Interval with what the sink will try to get batched requests from the records queue 
and send them to threads for sending to Sheets API.

**Flush Execution Timeout:** Maximal time the single thread should process the batched API request. 
Be careful, the number of retries and maximal retry time also should be taken into account.

### Advanced

**Minimal Page Extension Size:** Minimal size of sheet extension when default sheet size (1000) was exceeded.

**Merge Data Cells:** Toggle that defines should the sink merge data cells created as result of 
input arrays flatterning.