# Google Drive Batch Source


Description
-----------
Reads fileset from specified Google Drive directory vie Google Drive API.

Properties
----------
### Basic

**Directory identifier:** Identifier of the source folder.

**Filter:** Filter that can be applied to the files in the selected directory. 
Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).

**Modification date range:** Filter that narrows set of files by modified date range. 
User can select either among predefined or custom entered ranges. 
For _Custom_ selection there appear two additional fields: **Start date** and **End date**. 

**Start Date:** Accepts start date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field. 
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**End Date:** Accepts end date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification date range** field.
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**File properties:** Properties of files which should be transferred to output schema.

**File types to pull:** Types of files should be pulled from specified directory. 
Are supported following values: binary (all no-Google Drive formats), Google Documents, Google Spreadsheets, 
Google Drawings, Google Presentations and Google Apps Scripts. 
For Google Drive formats user should specify exporting format in **Exporting** section.

### Authentication

**Authentication type:** Defines the authentication type. OAuth2 and Service account types are available. 
Default is OAuth2.

**Client ID:** OAuth2 client id. Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Client secret:** OAuth2 client secret. 
Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Refresh token:** OAuth2 refresh token. 
Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Access token:** OAuth2 access token. 
Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Account file path:** Path on the local file system of the service account key used for authorization. 
Is shown only when _Service account_ auth type is selected for **Authentication type** property.
Can be set to 'auto-detect' when running on a Dataproc cluster. 
When running on other clusters, the file must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

### Advanced

**Maximum partition size:** Maximum body size for each partition specified in bytes. Default 0 value means unlimited.
Is not applicable for files in Google formats.

**Body output format** Output format for body of file. "Bytes" and "String" values are available. Default is "Bytes"

### Exporting

**Google Documents export format:** MIME type for exporting Google Documents. Default value is 'text/plain'.

**Google Spreadsheets export format:** MIME type for exporting Google Spreadsheets. Default value is 'text/csv'.

**Google Drawings export format:** MIME type for exporting Google Drawings. Default value is 'image/svg+xml'.

**Google Presentations export format:** MIME type for exporting Google Presentations. Default value is 'text/plain'.