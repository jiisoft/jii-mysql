/**
 * @author Vladimir Kozhin <affka@affka.ru>
 * @license MIT
 */

'use strict';

var Jii = require('jii');
var SqlQueryException = require('jii/data/SqlQueryException');
var _values = require('lodash/values');
var _map = require('lodash/map');
var BaseConnection = require('jii/data/BaseConnection');
var mysql = require('mysql');
var Schema = require('./Schema');

class Connection extends BaseConnection {

    preInit() {
        this._connection = null;

        /**
         * @type {string}
         */
        this.database = '';

        /**
         * @type {string}
         */
        this.port = 3306;

        /**
         * @type {string}
         */
        this.host = '127.0.0.1';

        super.preInit(...arguments);

        /**
         * @type {string}
         */
        this.driverName = 'mysql';

        /**
         * @type {string|object}
         */
        this.schemaClass = Schema;
    }

    /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * @protected
     */
    _initConnection() {
        this._connection = mysql.createConnection({
            host: this.host,
            port: this.port,
            user: this.username,
            password: this.password,
            database: this.database,
            timezone: 'local',
            typeCast: this._typeCast
        });
        this._connection.on('error', this._onError);
        this._connection.connect();

        if (this.charset !== null) {
            this.exec('SET NAMES ' + this.quoteValue(this.charset));
        }

        super._initConnection();
    }

    /**
     * @protected
     */
    _closeConnection() {
        if (this._connection) {
            this._connection.end();
        }
    }

    /**
     * Disable auto typing
     * @returns {string}
     * @private
     */
    _typeCast(field, next) {
        return field.string();
    }

    /**
     *
     * @param {string} message
     * @private
     */
    _onError(message) {}

    /**
     *
     * @param {string} sql
     * @param {string} [method]
     * @returns {Promise}
     */
    exec(sql, method) {
        method = method || null;

        return new Promise((resolve, reject) => {
            this._connection.query(sql, (err, rows) => {
                if (err) {
                    Jii.error('Database query error, sql: `' + sql + '`, error: ' + String(err));

                    if (method === 'execute' || method === null) {
                        reject(new SqlQueryException(err));
                    } else {
                        resolve(false);
                    }
                    return;
                }

                var result = null;
                switch (method) {
                    case 'execute':
                        result = {
                            affectedRows: rows.affectedRows,
                            insertId: rows.insertId || null
                        };
                        break;

                    case 'one':
                        result = rows.length > 0 ? rows[0] : null;
                        break;

                    case 'all':
                        result = rows;
                        break;

                    case 'scalar':
                        result = rows.length > 0 ? _values(rows[0])[0] : null;
                        break;

                    case 'column':
                        result = _map(rows, row => _values(row)[0]);
                        break;

                    default:
                        result = rows;
                }

                resolve(result);
            });
        });
    }

}
module.exports = Connection;