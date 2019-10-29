# Google Sheets Batch Source


Description
-----------
Reads spreadsheets from specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory identifier:** Identifier of the source folder.

**Sheets to pull:** List with available sheet names.

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
