# Simple promise-based Google Sheets Access from Nodejs

[![NPM version](https://badge.fury.io/js/google-sheets-node-api.png)](http://badge.fury.io/js/google-sheets-node-api)

A simple Node.js library to read and manipulate data in Google Spreadsheets.

Works without authentication for read-only sheets or with auth for adding/editing/deleting data.
Supports both list-based and cell-based feeds.

## Installation

`npm install google-sheets-node-api`



## Basic Usage

``` javascript
var creds = require('<client key JSON file>');

var GoogleSpreadsheet = require("google-sheets-node-api");

var mySheet = new GoogleSpreadsheet('<spreadsheet ID>');

mySheet.useServiceAccountAuth(creds).then(mySheet.getInfo.bind(mySheet)).then(function(sheet_info ) {
    console.log( sheet_info.title + ' is loaded' );

    var sheet1 = sheet_info.worksheets[0];

    sheet1.addRow({'Col1': 'Val1', Col2: 'Val2', Col3:'Val3', Col4: 'Val4', Col5: 'Val5', Col6: 'Val6', Col7: 'Val7'})
        .then(sheet1.getRows.bind(sheet1, null))
        .then(function(rows) {
            return [rows, rows[0].del()];
        })
        .spread(function(rows) {
            console.log('Done deleteing');
            rows[1].Col7 = 'new val2';
            return rows[1].save();
        })
        .then(console.log.bind(console, 'Done saving'))
        .catch(function(e) {
            console.error(e);
        });
});
```

## Authentication

IMPORTANT: Google recently deprecated their ClientLogin (username+password)
access, so things are slightly more complicated now. Older versions of this
module supported it, so just be aware that things changed.

### Unauthenticated access (read-only access on public docs)

By default, this module makes unauthenticated requests and can therefore
only access spreadsheets that are "public".

The Google Spreadsheets Data API reference and developers guide is a little
ambiguous about how you access a "published" public Spreadsheet.

If you wish to work with a Google Spreadsheet without authenticating, not only
must the Spreadsheet in question be visible to the web, but it must also have
been explicitly published using "File > Publish to the web" menu option in
the google spreadsheets GUI.

Many seemingly "public" sheets have not also been "published" so this may
cause some confusion.


### Service Account (recommended method)

This is a 2-legged oauth method and designed to be "an account that belongs to your application instead of to an individual end user".
Use this for an app that needs to access a set of documents that you have full access to.
([read more](https://developers.google.com/identity/protocols/OAuth2ServiceAccount))

__Setup Instructions__

1. Go to the [Google Developers Console](https://console.developers.google.com/project)
2. Select your project or create a new one (and then select it)
3. Enable the Drive API for your project
  - In the sidebar on the left, expand __APIs & auth__ > __APIs__
  - Search for "drive"
  - Click on "Drive API"
  - click the blue "Enable API" button
4. Create a service account for your project
  - In the sidebar on the left, expand __APIs & auth__ > __Credentials__
  - Click "Create new Client ID" button
  - select the "Service account" option
  - click "Create Client ID" button to continue
  - when the dialog appears click "Okay, got it"
  - your JSON key file is generated and downloaded to your machine (__it is the only copy!__)
  - note your service account's email address (also available in the JSON key file)
5. Share the doc (or docs) with your service account using the email noted above


## API

### `GoogleSpreadsheet`

The main class that represents an entire spreadsheet.


#### `new GoogleSpreadsheet(sheet_id, [auth], [options])`

Create a new google spreadsheet object.

- `sheet_id` -- the ID of the spreadsheet (from its URL)
- `auth` - (optional) an existing auth token
- `options` - (optional)
  - `visibility` - defaults to `public` if anonymous
  - `projection` - defaults to `values` if anonymous



#### `GoogleSpreadsheet.useServiceAccountAuth(account_info)`

Uses a service account email and public/private key to create a token to use to authenticated requests.
Normally you would just pass in the require of the json file that google generates for you when you create a service account.

See the "Authentication" section for more info.

If you are using heroku or another environment where you cannot save a local file, you may just pass in an object with
- `client_email` -- your service account's email address
- `private_key` -- the private key found in the JSON file

Internally, this uses a JWT client to generate a new auth token for your service account that is valid for 1 hour. The token will be automatically regenerated when it expires.



#### `GoogleSpreadsheet.setAuthToken(id)`

Use an already created auth token for all future requests.



#### `GoogleSpreadsheet.getInfo()`

Get information about the spreadsheet. Returns a promise which inturn will return the following object when fulfilled.

- `title` - the title of the document
- `updated` - last updated timestamp
- `author` - auth info in an object
  - `name` - author name
  - `email` - author email
- `worksheets` - an array of `SpreadsheetWorksheet` objects (see below)



#### `GoogleSpreadsheet.getRows(worksheet_id, options)`

Returns an array of row objects from the sheet when the promise is fulfilled.

- `worksheet_id` - the index of the sheet to read from (index starts at 1)
- `options` (optional)
  - `start-index` - start reading from row #
  - `max-results` - max # of rows to read at once
  - `orderby` - column key to order by
  - `reverse` - reverse results
  - `query` - send a structured query for rows ([more info](https://developers.google.com/google-apps/spreadsheets/#sending_a_structured_query_for_rows))



#### `GoogleSpreadsheet.addRow(worksheet_id, new_row)`

Adds the new row to the sheet when the promise is fulfilled.

- `worksheet_id` - the index of the sheet to add to (index starts at 1)
- `new_row` - key-value object to add - keys must match the header row on your sheet



#### `GoogleSpreadsheet.getCells(worksheet_id, options)`

Returns an array of cell objects when the promise is fulfilled.

- `worksheet_id` - the index of the sheet to add to (index starts at 1)
- `options` (optional)
  - `min-row` - row range min (uses #s visible on the left)
  - `max-row` - row range max
  - `min-col` - column range min (uses numbers, not letters!)
  - `max-col` - column range max
  - `return-empty` - include empty cells (boolean)


----------------------------------

### `SpreadsheetWorksheet`

Represents a single "sheet" from the spreadsheet. These are the different tabs/pages visible at the bottom of the Google Sheets interface.

This is a really just a wrapper to call the same functions on the spreadsheet without needing to include the worksheet id.

__Properties:__
- `id` - the ID of the sheet
- `title` - the title (visible on the tabs in google's interface)
- `rowCount` - number of rows
- `colCount` - number of columns

### `SpreadsheetWorksheet.getRows(options)`
See above.

### `SpreadsheetWorksheet.getCells(options)`
See above.

### `SpreadsheetWorksheet.addRow(new_row)`
See above.

----------------------------------

### `SpreadsheetRow`
Represents a single row from a sheet.

You can treat the row as a normal javascript object. Object keys will be from the header row of your sheet, however the google API mangles the names a bit to make them simpler. It's easiest if you just use all lowercase keys to begin with.

#### `SpreadsheetRow.save()`
Saves any changes made to the row's values.

#### `SpreadsheetRow.del()`
Deletes the row from the sheet.

----------------------------------

### `SpreadsheetCell`
Represents a single cell from the sheet.

#### `SpreadsheetCell.setValue(val)`
Set the value of the cell and save it.

#### `SpreadsheetCell.del()`
Clear the cell -- internally just calls `.setValue('')`


----------------------------------

## Further possibilities & to-do

- batch requests for cell based updates
- modifying worksheet/spreadsheet properties
- getting list of available spreadsheets for an authenticated user

## Links

- <https://developers.google.com/google-apps/spreadsheets/>
- <https://github.com/Ajnasz/GoogleClientLogin>


## Thanks
This is a rewrite of [node-google-spreadsheet](https://github.com/theoephraim/node-google-spreadsheet), so thanks to [Theo Ephraim](https://github.com/theoephraim)
Also big thanks fo GoogleClientLogin for dealing with authentication.


## License
google-sheets-node-api is free and unencumbered public domain software. For more information, see the accompanying UNLICENSE file.
