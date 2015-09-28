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
var GooogleSpreadsheet = function (ss_key, auth_id, options) {
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
    this.makeFeedRequest = function (url_params, method, query_or_data) {
        var url;
        var headers = {};

        return Q().then(function () {
            if (typeof (url_params) == 'string') {
                // used for edit / delete requests
                url = url_params;
            } else if (Array.isArray(url_params)) {
                //used for get and post requets
                url_params.push(visibility, projection);
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

    // public API methods
    this.getInfo = function () {
        return self.makeFeedRequest(["worksheets", ss_key], 'GET', null).spread(function (data, xml) {
            if (data === true) {
                throw new Error('No response to getInfo call');
            }
            var ss_data = {
                title: data.title["_"],
                updated: data.updated,
                author: data.author,
                worksheets: []
            };
            var worksheets = forceArray(data.entry);
            worksheets.forEach(function (ws_data) {
                ss_data.worksheets.push(new SpreadsheetWorksheet(self, ws_data));
            });
            return ss_data;
        });
    };

    // NOTE: worksheet IDs start at 1

    this.getRows = function (worksheet_id, opts) {
        // the first row is used as titles/keys and is not included

        // opts is optional
        opts = opts || {};


        var query = {};
        if (opts.start) query["start-index"] = opts.start;
        if (opts.num) query["max-results"] = opts.num;
        if (opts.orderby) query["orderby"] = opts.orderby;
        if (opts.reverse) query["reverse"] = opts.reverse;
        if (opts.query) query['sq'] = opts.query;

        return self.makeFeedRequest(["list", ss_key, worksheet_id], 'GET', query).spread(function (data, xml) {
            if (data === true) {
                throw new Error('No response to getRows call');
            }
            xml = xml.toString('utf-8');
            // gets the raw xml for each entry -- this is passed to the row object so we can do updates on it later
            var entries_xml = xml.match(/<entry[^>]*>([\s\S]*?)<\/entry>/g);
            var rows = [];
            var entries = forceArray(data.entry);
            var i = 0;
            entries.forEach(function (row_data) {
                rows.push(new SpreadsheetRow(self, row_data, entries_xml[i++]));
            });

            return rows;
        });
    };

    this.addRow = function (worksheet_id, data) {
        var data_xml = '<entry xmlns="http://www.w3.org/2005/Atom" xmlns:gsx="http://schemas.google.com/spreadsheets/2006/extended">' + "\n";
        Object.keys(data).forEach(function (key) {
            if (key != 'id' && key != 'title' && key != 'content' && key != '_links') {
                data_xml += '<gsx:' + xmlSafeColumnName(key) + '>' + xmlSafeValue(data[key]) + '</gsx:' + xmlSafeColumnName(key) + '>' + "\n"
            }
        });
        data_xml += '</entry>';
        return self.makeFeedRequest(["list", ss_key, worksheet_id], 'POST', data_xml);
    };

    this.getCells = function (worksheet_id, opts) {
        // opts is optional
        opts = opts || {};

        // Supported options are:
        // min-row, max-row, min-col, max-col, return-empty
        var query = _.extend({}, opts);

        return self.makeFeedRequest(["cells", ss_key, worksheet_id], 'GET', query).spread(function (data, xml) {
            if (data === true) {
                throw new Error('No response to getCells call');
            }

            var cells = [];
            var entries = forceArray(data['entry']);

            entries.forEach(function (cell_data) {
                cells.push(new SpreadsheetCell(self, worksheet_id, cell_data));
            });

            return cells;
        });
    }
};

// Classes
var SpreadsheetWorksheet = function (spreadsheet, data) {
    var self = this;

    self.id = data.id.substring(data.id.lastIndexOf("/") + 1);
    self.title = data.title["_"];
    self.rowCount = data['gs:rowCount'];
    self.colCount = data['gs:colCount'];

    this.getRows = function (opts) {
        return spreadsheet.getRows(self.id, opts);
    };
    this.getCells = function (opts) {
        return spreadsheet.getCells(self.id, opts);
    };
    this.addRow = function (data) {
        return spreadsheet.addRow(self.id, data);
    }
};

var SpreadsheetRow = function (spreadsheet, data, xml) {
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
                val = forceArray(val);
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
                data_xml = data_xml.replace(new RegExp('<gsx:' + xmlSafeColumnName(key) + ">([\\s\\S]*?)</gsx:" + xmlSafeColumnName(key) + '>'), '<gsx:' + xmlSafeColumnName(key) + '>' + xmlSafeValue(self[key]) + '</gsx:' + xmlSafeColumnName(key) + '>');
            }
        });
        return spreadsheet.makeFeedRequest(self['_links']['edit'], 'PUT', data_xml);
    };

    self.del = function () {
        return spreadsheet.makeFeedRequest(self['_links']['edit'], 'DELETE', null);
    }
};

var SpreadsheetCell = function (spreadsheet, worksheet_id, data) {
    var self = this;

    self.id = data['id'];
    self.row = parseInt(data['gs:cell']['$']['row']);
    self.col = parseInt(data['gs:cell']['$']['col']);
    self.value = data['gs:cell']['_'];
    self.numericValue = data['gs:cell']['$']['numericValue'];

    self['_links'] = [];
    var links = forceArray(data.link);
    links.forEach(function (link) {
        self['_links'][link['$']['rel']] = link['$']['href'];
    });

    self.setValue = function (new_value) {
        self.value = new_value;
        return self.save();
    };

    self.save = function () {
        var new_value = xmlSafeValue(self.value);
        var edit_id = 'https://spreadsheets.google.com/feeds/cells/key/worksheetId/private/full/R' + self.row + 'C' + self.col;
        var data_xml =
            '<entry><id>' + edit_id + '</id>' +
            '<link rel="edit" type="application/atom+xml" href="' + edit_id + '"/>' +
            '<gs:cell row="' + self.row + '" col="' + self.col + '" inputValue="' + new_value + '"/></entry>';

        data_xml = data_xml.replace('<entry>', "<entry xmlns='http://www.w3.org/2005/Atom' xmlns:gs='http://schemas.google.com/spreadsheets/2006'>");

        return spreadsheet.makeFeedRequest(self['_links']['edit'], 'PUT', data_xml);
    };

    self.del = function () {
        return self.setValue('');
    }
};

//utils
var forceArray = function (val) {
    if (Array.isArray(val)) return val;
    if (!val) return [];
    return [val];
};
var xmlSafeValue = function (val) {
    if (val == null) return '';
    return String(val).replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
};
var xmlSafeColumnName = function (val) {
    if (!val) return '';
    return String(val).replace(/[\s_]+/g, '')
        .toLowerCase();
};

module.exports = GooogleSpreadsheet;
