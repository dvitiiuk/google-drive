# Google Drive Batch Source


Description
-----------
Reads a fileset from specified Google Drive directory via Google Drive API.

Properties
----------
### Basic

**Directory identifier:** Identifier of the source folder.

**File metadata properties:** Properties that represent metadata of files. 
They will be a part of output structured record. Descriptions for properties can be view at 
[Drive API file reference](https://developers.google.com/drive/api/v3/reference/files).

### Filtering

**Filter:** Filter that can be applied to the files in the selected directory. 
Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).

**Modification date range:** Filter that narrows set of files by modified date range. 
User can select either among predefined or custom entered ranges. 
For _Custom_ selection the dates range can be specified via **Start date** and **End date**. 

**Start Date:** Start date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field. 
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**End Date:** End date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field.
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**File types to pull:** Types of files which should be pulled from a specified directory. 
The following values are supported: binary (all non-Google Drive formats), Google Documents, Google Spreadsheets, 
Google Drawings, Google Presentations and Google Apps Scripts. 
For Google Drive formats user should specify exporting format in **Exporting** section.

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
Can be set to 'auto-detect' when running on a Dataproc cluster. 
When running on other clusters, the file must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

### Advanced

**Maximum partition size:** Maximum body size for each structured record specified in bytes. 
Default 0 value means unlimited. Is not applicable for files in Google formats.

**Body output format** Output format for body of file. "Bytes" and "String" values are available.

### Exporting

**Google Documents export format:** MIME type which is used for Google Documents when converted to structured records.

**Google Spreadsheets export format:** MIME type which is used for Google Spreadsheets when converted to structured records.

**Google Drawings export format:** MIME type which is used for Google Drawings when converted to structured records.

**Google Presentations export format:** MIME type which is used for Google Presentations when converted to structured records.