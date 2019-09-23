# Google Drive Batch Source


Description
-----------
Reads fileset from specified Google Drive directory vie Google Drive API.

Properties
----------
### Basic

**appId** Oauth2 app id

**accessToken** OAuth2 access token

**directoryIdentifier** ID of the destination folder

**filter** A filter that can be applied to the files in the selected directory. Filters follow the Google Drive filter syntax.

**modificationDateRange** Filters files to only pull those that were modified between the date range.

**startDate** Accepts start date for modification date range. RFC3339 format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**endDate** Accepts end date for modification date range. RFC3339 format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**fileProperties** Properties which should be get for each file in directory.

**fileTypesToPull** Types of files should be pulled from specified directory.

### Advanced

**maxPartitionSize** Maximum body size for each partition specified in bytes. Default 0 value means unlimited.

### Exporting

**docsExportingFormat** MIME type for Google Documents.

**sheetsExportingFormat** MIME type for Google Spreadsheets.

**drawingsExportingFormat** MIME type for Google Drawings.

**presentationsExportingFormat** MIME type for Google Presentations.