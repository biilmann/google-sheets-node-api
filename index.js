var Q = require("q");
var https = require('q-io/http');
var request = require("request");
var xml2js = require("xml2js");
var http = require("http");
var querystring = require("querystring");
var _ = require('underscore');
var GoogleAuth = require("google-auth-library");

var GOOGLE_FEED_URL = "https://spreadsheets.google.com/feeds/";
var GOOGLE_AUTH_SCOPE = ["https://spreadsheets.google.com/feeds"];

// The main class that represents a single sheet
// this is the main module.exports

module.exports = function(ss_key, auth_id, options) {
    var self = this;
    var google_auth = null;
    var visibility = 'public';
    var projection = 'values';

    var auth_mode = 'anonymous';

    var auth_client = new GoogleAuth();
    var jwt_client;

    options = options || {};

    var xml_parser = new xml2js.Parser({
        // options carried over from older version of xml2js
        // might want to update how the code works, but for now this is fine
        explicitArray: false,
        explicitRoot: false
    });

    if (!ss_key) {
        throw new Error("Spreadsheet key not provided.");
    }

    function setAuthAndDependencies(auth) {
        google_auth = auth;
        if (!options.visibility) {
            visibility = google_auth ? 'private' : 'public';
        }
        if (!options.projection) {
            projection = google_auth ? 'full' : 'values';
        }
    }

    // auth_id may be null
    setAuthAndDependencies(auth_id);

    // Authentication Methods

    this.setAuthToken = function (auth_id) {
        if (auth_mode == 'anonymous') auth_mode = 'token';
        setAuthAndDependencies(auth_id);
    };

    this.useServiceAccountAuth = function (creds) {
        if (typeof creds == 'string') creds = require(creds);
        jwt_client = new auth_client.JWT(creds.client_email, null, creds.private_key, GOOGLE_AUTH_SCOPE, null);
        return renewJwtAuth();
    };

    function renewJwtAuth() {
        auth_mode = 'jwt';
        return Q.ninvoke(jwt_client, 'authorize').then(function (token) {
            self.setAuthToken({
                type: token.token_type,
                value: token.access_token,
                expires: token.expiry_date
            });
            return null;
        });
    }

    // This method is used internally to make all requests
    this.makeFeedRequest = function (url_params, urlParams2, method, query_or_data) {
        var url;
        var headers = {};

        return Q().then(function () {
            if (typeof (url_params) == 'string') {
                // used for edit / delete requests
                url = url_params;
            } else if (Array.isArray(url_params)) {
                //used for get and post requets
                url_params.push(visibility, projection);
                if (urlParams2) {
                    url_params = url_params.concat(urlParams2);
                }
                url = GOOGLE_FEED_URL + url_params.join("/");
            }

            if (auth_mode != 'jwt') return null;
            // check if jwt token is expired
            if (google_auth.expires > +new Date()) return null;
            return renewJwtAuth();

        }).then(function () {
            if (google_auth) {
                if (google_auth.type === 'Bearer') {
                    headers['Authorization'] = 'Bearer ' + google_auth.value;
                } else {
                    headers['Authorization'] = "GoogleLogin auth=" + google_auth;
                }
            }

            if (method == 'POST' || method == 'PUT') {
                headers['content-type'] = 'application/atom+xml';
            }

            if (method == 'GET' && query_or_data) {
                url += "?" + querystring.stringify(query_or_data);
            }
            //
            return https.request({
                url: url,
                method: method,
                headers: headers,
                body: method == 'POST' || method == 'PUT' ? [query_or_data] : null
            });

        }).then(function (res) {
            return [res, res.body.read()];
        }).spread(function (res, body) {
            if (res.status === 401) {
                throw new Error("Invalid authorization key. " + body);
            } else if (res.status >= 400) {
                throw new Error("HTTP error " + res.status + ": " + http.STATUS_CODES[res.status] + '. ' + body);
            } else if (res.status === 200 && res.headers['content-type'].indexOf('text/html') >= 0) {
                throw new Error("Sheet is private. Use authentication or make public. (see https://github.com/theoephraim/node-google-spreadsheet#a-note-on-authentication for details)\n" + body);
            }
            return body;
        }).then(function (xml) {
            return xml ? [Q.ninvoke(xml_parser, 'parseString', xml), xml] : [null, null];
        }).spread(function (json, xml) {
            return [json, xml];
        });
    };

    this.getSpreadsheet = function () {
        return self.makeFeedRequest(["worksheets", ss_key], null, 'GET').spread(function (data, xml) {
            if (data === true) {
                throw new Error('No response to getWorksheets call');
            }
            return new Spreadsheet(self, data);
        });
    };

    // NOTE: worksheet IDs start at 1
    this.addWorksheet = function(xml) {
        return self.makeFeedRequest(['worksheets', ss_key], null, 'POST', xml);
    };
    
    this.updateWorksheetMetadata = function(worksheetUrl, xml) {
        return self.makeFeedRequest(worksheetUrl, null, 'PUT', xml);
    };

    this.deleteWorksheet = function(worksheetUrl) {
        return self.makeFeedRequest(worksheetUrl, null, 'DELETE');
    };

    this.saveWorksheet = function(worksheetId, xml) {
        return self.makeFeedRequest(["cells", ss_key, worksheetId], ['batch'], 'POST', xml);
    };

    this.getRows = function (worksheetId, opts) {
        // the first row is used as titles/keys and is not included
        return self.makeFeedRequest(["list", ss_key, worksheetId], null, 'GET', opts || {});
    };

    this.addRow = function (worksheetId, xml) {
        return self.makeFeedRequest(["list", ss_key, worksheetId], null, 'POST', xml);
    };

    this.deleteRow = function(rowUrl) {
        return self.makeFeedRequest(rowUrl, null, 'DELETE');
    };

    this.saveRow = function(rowUrl, xml) {
        return self.makeFeedRequest(rowUrl, null, 'PUT', xml);
    }

    this.getCells = function (worksheetId, opts) {
        return self.makeFeedRequest(["cells", ss_key, worksheetId], null, 'GET', opts || {});
    };

    this.saveCell = function(cellUrl, xml) {
        return self.makeFeedRequest(cellUrl, null, 'PUT', xml);
    };
};

// Classes
var Spreadsheet = function(gsheet, data) {
    var self = this;

    this.title = data.title["_"];
    this.updated = data.updated;
    this.author = data.author;
    this.worksheets = toArray(data.entry).map(function (wsdata) {
        return new Worksheet(gsheet, self, wsdata);
    });

    this.addWorksheet = function(name, rows, cols) {
        var xml = '<entry xmlns="http://www.w3.org/2005/Atom" xmlns:gs="http://schemas.google.com/spreadsheets/2006">';
        xml += '<title>' + name + '</title>';
        xml += '<gs:rowCount>' + rows + '</gs:rowCount>';
        xml += '<gs:colCount>' + cols + '</gs:colCount>';
        xml += '</entry>';
        return gsheet.addWorksheet(xml).then(function(wsdata) {
            var worksheet = new Worksheet(gsheet, self, wsdata[0]);
            self.worksheets.push(worksheet);
            return worksheet;
        });
    };

    this.deleteWorksheet = function(worksheet) {
        return gsheet.deleteWorksheet(worksheet.links.edit.href).then(function() {
            return self.worksheets = self.worksheets.filter(function(wsheet) {
                return wsheet != worksheet;
            });
        });
    };
};

var Worksheet = function(gsheet, spreadsheet, data) {
    var self = this;

    self.id = data.id.substring(data.id.lastIndexOf('/') + 1);
    self.title = data.title['_'];
    self.rowCount = data['gs:rowCount'];
    self.colCount = data['gs:colCount'];
    self.cells = [];
    self.links = toArray(data.link).reduce(function(links, l) {
        l = l['$'];
        links[l.rel.substring(l.rel.lastIndexOf('#') + 1)] = l;
        return links;
    }, {});
    
    this.updateMetadata = function(title, rows, cols) {
        var xml = '<entry xmlns="http://www.w3.org/2005/Atom" xmlns:gs="http://schemas.google.com/spreadsheets/2006">';
        xml += '<title>' + title + '</title>';
        xml += '<gs:rowCount>' + rows + '</gs:rowCount>';
        xml += '<gs:colCount>' + cols + '</gs:colCount>';
        xml += '</entry>';

        return gsheet.updateWorksheetMetadata(self.links.edit.href, xml).then(function() {
            self.title = title;
            self.rowCount = rows;
            self.colCount = cols;
            return self;
        });
    };

    /**
     * @param opts - Supported options are:
     *      start-index: starting index
     *      max-results: max # of rows returned
     *      orderby: sort by column
     *      reverse: sort in reverse order
     *      sq: query
     * @returns {*}
     */
    this.getRows = function (opts) {
        return gsheet.getRows(self.id, opts).spread(function (data, xml) {
            if (data === true) {
                throw new Error('No response to getRows call');
            }
            xml = xml.toString('utf-8');
            // gets the raw xml for each entry -- this is passed to the row object so we can do updates on it later
            var entries_xml = xml.match(/<entry[^>]*>([\s\S]*?)<\/entry>/g);
            var rows = [];
            var entries = toArray(data.entry);
            var i = 0;
            entries.forEach(function (row_data) {
                rows.push(new WorksheetRow(gsheet, self, row_data, entries_xml[i++]));
            });
            return rows;
        });
    };
    /**
     * @param opts - Supported options are:
     *      min-row
     *      max-row
     *      min-col
     *      max-col
     *      return-empty (false): Include empty cells in the results
     * @returns {*}
     */
    this.getCells = function (opts) {
        return gsheet.getCells(self.id, opts).spread(function (data, xml) {
            if (data === true) {
                throw new Error('No response to getCells call');
            }

            return self.cells = toArray(data['entry']).map(function (cell_data) {
                return new WorksheetCell(gsheet, self, cell_data);
            });
        });
    };
    this.addRow = function (data) {
        var xml = '<entry xmlns="http://www.w3.org/2005/Atom" xmlns:gsx="http://schemas.google.com/spreadsheets/2006/extended">' + "\n";
        Object.keys(data).forEach(function (key) {
            if (key != 'id' && key != 'title' && key != 'content' && key != '_links') {
                xml += '<gsx:' + encodeColName(key) + '>' + encodeXml(data[key]) + '</gsx:' + encodeColName(key) + '>' + "\n"
            }
        });
        xml += '</entry>';
        return gsheet.addRow(self.id, xml);
    };
    this.save = function() {
        if (!self.cells) return Q();
        var xml = '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:batch="http://schemas.google.com/gdata/batch" xmlns:gs="http://schemas.google.com/spreadsheets/2006">';
        xml += '<id>' + self.links.cellsfeed.href + '</id>';
        xml = self.cells.reduce(function(xml, cell) {
            return xml + cell.batchEntry();
        }, xml);
        xml += '</feed>';
        return gsheet.saveWorksheet(self.id, xml);
    };

    this.delete = function() {
        return spreadsheet.deleteWorksheet(self);
    };
};

var WorksheetRow = function (gsheet, worksheet, data, xml) {
    var self = this;
    self['_xml'] = xml;
    Object.keys(data).forEach(function (key) {
        var val = data[key];
        if (key.substring(0, 4) === "gsx:") {
            if (typeof val === 'object' && Object.keys(val).length === 0) {
                val = null;
            }
            if (key == "gsx:") {
                self[key.substring(0, 3)] = val;
            } else {
                self[key.substring(4)] = val;
            }
        } else {
            if (key == "id") {
                self[key] = val;
            } else if (val['_']) {
                self[key] = val['_'];
            } else if (key == 'link') {
                self['_links'] = [];
                val = toArray(val);
                val.forEach(function (link) {
                    self['_links'][link['$']['rel']] = link['$']['href'];
                });
            }
        }
    }, this);

    self.save = function () {
        /*
         API for edits is very strict with the XML it accepts
         So we just do a find replace on the original XML.
         It's dumb, but I couldnt get any JSON->XML conversion to work reliably
         */

        var data_xml = self['_xml'];
        // probably should make this part more robust?
        data_xml = data_xml.replace('<entry>', "<entry xmlns='http://www.w3.org/2005/Atom' xmlns:gsx='http://schemas.google.com/spreadsheets/2006/extended'>");
        Object.keys(self).forEach(function (key) {
            if (key.substr(0, 1) != '_' && typeof (self[key] == 'string')) {
                data_xml = data_xml.replace(new RegExp('<gsx:' + encodeColName(key) + ">([\\s\\S]*?)</gsx:" + encodeColName(key) + '>'), '<gsx:' + encodeColName(key) + '>' + encodeXml(self[key]) + '</gsx:' + encodeColName(key) + '>');
            }
        });
        return gsheet.saveRow(self['_links']['edit'], data_xml);
    };

    self.delete = function () {
        return gsheet.deleteRow(self['_links']['edit']);
    }
};

var WorksheetCell = function(gsheet, worksheet, data) {
    var self = this;

    self.id = data['id'];
    self.row = parseInt(data['gs:cell']['$']['row']);
    self.col = parseInt(data['gs:cell']['$']['col']);
    self.value = data['gs:cell']['_'];
    self.numericValue = data['gs:cell']['$']['numericValue'];
    self._links = toArray(data.link).reduce(function(links, l) {
        l = l['$'];
        links[l.rel.substring(l.rel.lastIndexOf('#') + 1)] = l;
        return links;
    }, {});

    self.worksheet = worksheet;

    self.setValue = function(new_value) {
        self.value = new_value;
        self.dirty = true;
    };

    self.getValueAsColName = function() {
        if (!self.value) return '';
        return String(self.value).replace(/\s+/g, '_').replace(/[^\w]+/g, '').toLowerCase();
    };

    self.batchEntry = function(operation) {
        if (!self.dirty) return '';
        var xml = '<entry><batch:id>R' + self.row + 'C' + self.col + '</batch:id>';
        xml += '<batch:operation type="' + (operation || 'update') + '" />';
        xml += '<id>' + self.id + '</id>';
        xml += '<link rel="edit" type="application/atom+xml" href="' + self._links.edit.href + '"/>';
        xml += '<gs:cell row="' + self.row + '" col="' + self.col + '" inputValue="' + encodeXml(self.value) + '" />';
        xml += '</entry>';
        return xml;
    };

    self.save = function () {
        var new_value = encodeXml(self.value);
        var edit_id = 'https://spreadsheets.google.com/feeds/cells/key/worksheetId/private/full/R' + self.row + 'C' + self.col;
        var data_xml =
            '<entry><id>' + self.id + '</id>' +
            '<link rel="edit" type="application/atom+xml" href="' + self._links.edit.href + '"/>' +
            '<gs:cell row="' + self.row + '" col="' + self.col + '" inputValue="' + new_value + '"/></entry>';

        data_xml = data_xml.replace('<entry>', "<entry xmlns='http://www.w3.org/2005/Atom' xmlns:gs='http://schemas.google.com/spreadsheets/2006'>");

        return gsheet.saveCell(self['_links']['edit'].href, data_xml);
    };
};

//utils
var toArray = function (val) {
    if (Array.isArray(val)) return val;
    if (!val) return [];
    return [val];
};
var encodeXml = function (val) {
    if (val == null) return '';
    return String(val).replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
};
var encodeColName = function (val) {
    if (!val) return '';
    //return String(val).replace(/\s+/g, '_').replace(/[^\w]+/g, '').toLowerCase();
    return String(val).replace(/[\s_]+/g, '').replace(/[^\w]+/g, '').toLowerCase();
};
