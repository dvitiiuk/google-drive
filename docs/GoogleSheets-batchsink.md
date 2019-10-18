# Google Drive Batch Source


Description
-----------
Reads spreadsheets from specified Google Drive directory via Google Sheets API.

Properties
----------
### Basic

**Directory identifier:** Identifier of the source folder.

**Sheet name:** Name of the .

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
