# Google Drive Batch Source


Description
-----------
Reads spreadsheets from specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory identifier:** Identifier of the source folder.

**Spreadsheet name field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set Google API will use the value of **Default Spreadsheet name** property.

**Default Spreadsheet name:** Default spreadsheet file name. 
Is used when user don't specify schema field as spreadsheet name.

**Sheet name field:** Name of the schema field (should be STRING type) which will be used as sheet title. 
Is optional. In the case it is not set Google API will use the value of **Default sheet name** property.

**Default sheet name:** Default sheet title. Is used when user don't specify schema field as sheet title.

**Write schema as first row:** Toggle that defines should the sink write out the input schema as first row of an 
out sheet.

### Authentication

**Authentication type:** Type of authentication used to access Google API. 
OAuth2 and Service account types are available.

#### OAuth2 properties

**Client ID:** OAuth2 client id.

**Client secret:** OAuth2 client secret.

**Refresh token:** OAuth2 refresh token.

**Access token:** OAuth2 access token.

#### Service account properties

**Account file path:** Path on the local file system of the service account key used for authorization. 
Can be set to 'auto-detect' for getting service account from system variable.
The file/system variable must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

### Retrying

**Max retry count:** Maximum number of retry attempts.

**Max retry wait:** Maximum wait time for attempt in seconds.

**Max retry jitter wait:** Maximum additional wait time is milliseconds.

### Buffering and paralellization

**Threads number:** Number of threads which send batched API requests. 
The greater value allows to process records quickly, but requires extended Google Sheets API quota.

**Maximal buffer size:** Maximal size in records of the batch API request. 
The greater value allows to reduce the number of API requests, but causes growing of their size.

**Records queue length:** Size of the queue used to receive records and for onwards grouping of them to 
batched API requests. For the greater value there is more likely that the sink will group arrived records in the 
butches of maximal size. Also greater value means more memory consumption.

**Maximum flush interval (s):** Interval with what the sink will try to get batched requests from the records queue 
and send them to threads for sending to Sheets API.

**Flush execution timeout (s):** Maximal time the single thread should process the batched API request. 
Be careful, the number of retries and maximal retry time also should be taken into account.

### Advanced

**Minimal page extension size:** Minimal size of sheet extension when default sheet size (1000) was exceeded.

**Merge data cells:** Toggle that defines should the sink merge data cells created as result of 
input arrays flatterning.