# Google Sheets Batch Source


Description
-----------
Reads spreadsheets from specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the source folder.

### Filtering

**Filter:** Filter that can be applied to the files in the selected directory. 
Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).

**Modification Date Range:** Filter that narrows set of files by modified date range. 
User can select either among predefined or custom entered ranges. 
For _Custom_ selection the dates range can be specified via **Start date** and **End date**. 

**Start Date:** Start date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field. 
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**End Date:** End date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field.
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**Sheets To Pull:** Filter for specifying set of sheets to process. 
For 'numbers' or 'titles' selections user can populate specific values in 'Sheets identifiers' field.

**Sheets Identifiers:** Set of sheets' numbers/titles to process. 
Is shown only when 'titles' or 'numbers' are selected for 'Sheets to pull' field.

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

### Metadata Extraction

**Extract Metadata:** Field to enable metadata extraction. Metadata extraction is useful when user wants to specify 
a header or a footer for a sheet. The rows in headers and footers are not available as data records. 
Instead, they are available in every record as a field called 'metadata', which is a record of the specified metadata.

**Metadata Record Name:** Name of the record with metadata content. 
It is needed to distinguish metadata record from possible column with the same name.

**First Header Row:** Row number of the first row to be treated as header.

**Last Header Row:** Row number of the last row to be treated as header.

**First Footer Row:** Row number of the first row to be treated as footer.

**Last Footer Row:** Row number of the last row to be treated as footer.

**Metadata Cells:** Set of the cells for key-value pairs to extract as metadata from the specified metadata sections.
Only shown if Extract metadata is set to true. The cell numbers should be within the header or footer.

### Advanced

**Text Formatting:** Output format for numeric sheet cells. 
In 'Formatted values' case the value will contain appropriate format of source cell e.g. '1.23$', '123%'." 
For 'Values only' only number value will be returned.

**Skip Empty Data:** Field to allow skipping of empty structure records.

**Column Names Selection:** Source for column names.

**Custom Row For Column Names:** Row number of the row to be treated as a header.
Only shown when the 'Column Names Selection' field is set to 'Custom row as column names' header.

**Last Data Column:** Last column number of the maximal field of plugin work for data.

**Last Data Row:** Last row number of the maximal field of plugin work for data.