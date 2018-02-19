/* jshint node:true */ /* global define, escape, unescape */
'use strict';

class YourSql {
	constructor() {
		this.mysql = require('mysql');
		this.pool = null;
	}


	init({ host, user, password, database, connectionLimit, debug }) {
		this.pool = this.mysql.createPool({
			connectionLimit: connectionLimit || 100,
			host: host,
			user: user,
			password: password,
			database: database,
			debug: debug
		});
	}


	hasResults(rows) {
		return rows !== undefined && rows !== null && rows.length !== null && rows.length !== undefined && rows.length > 0;
	}

	log(msg, err) {
		if (this.debug) {
			if (err !== undefined) {
				console.error(msg);
				console.error(err);
			}
			else {
				this.log(msg);
			}
		}
	}

	createDatabase(database, callback) {
		this.pool.getConnection((err, connection) => {
			if (err) {
				this.log('Error in connection to database', err);
				callback(err);
			}

			this.log('connected as id ' + connection.threadId);

			connection.query(`SHOW DATABASES LIKE '${database}'`, (err, rows) => {
				if (!err) {
					if (!this.hasResults(rows)) {
						this.log('Begin create schema ' + database);
						connection.query('CREATE SCHEMA ' + database, function(err, rows) {
							connection.release();
							if (err) {
								this.log('Error creating schema ' + database, err);
								callback(err);
							}
							else {
								this.log('Done creating schema - ' + database);
								callback(null, rows);
							}
						});
					}
					else {
						const msg = 'Schema already exists: ' + database;
						callback('Schema already exists: ' + database);
					}
				}
				else {
					connection.release();
					this.log('Error creating checking for schema: ' + database, err);
					callback(err);
				}

			});

			connection.on('error', function(err) {
				this.log('Error during while connecting to database.', err);
				return;
			});
		});
	}


	getSchemaSizeInMb(schema, callback) {
		var sizeQuery = 'SELECT Round(Sum(data_length + index_length) / 1024 / 1024, 1) "mb" FROM information_schema.tables WHERE table_schema = "' + schema + '"';
		this.query(sizeQuery, function(err, results) {
			if (err) {
				this.log('Error querying schema size', err);
				callback(err);
			}
			else {
				if (results === undefined || results === null || results.results === undefined || results.results === null || results.results[0] === undefined || results.results[0] === null) {
					callback('No results for schema ' + schema);
				}
				else {
					callback(null, results.results[0].mb);
				}
			}
		})
	}


	createUniqueConstraint(schema, tableName, columns, callback) {
		const indexName = `uk_${tableName}_${columns.join('_')}`;
		const uniqueConstraintStatement = `alter table ${schema}.${tableName} add constraint ${indexName} UNIQUE (${columns.join(',')})`;

		const doesIndexExistQuery = `select distinct index_name from information_schema.statistics 
			        where table_schema = '${schema}' 
			        and table_name = '${tableName}' and index_name = '${indexName}'`;

		this.query(doesIndexExistQuery, (err, rows) => {
			if (err) {
				this.log('error checking if uniqueConstraint exists: ' + indexName, err);
				callback(err);
			}
			else if (rows !== undefined && rows !== null && this.hasResults(rows.results)) {
				callback(indexName + ' already exists');
			}
			else {
				this.query(uniqueConstraintStatement, callback);
			}
		});
	}


	query(sql, callback) {
		if (sql !== undefined && sql !== null && sql.length > 0) {
			callback({
				err: '"sql" paremeter was not provided'
			});
			return;
		}
		
		try {
			this.pool.getConnection((err, connection) => {
				connection.release();
				if (err) {
					this.log('Error in connection to database', err);
					callback({
						err: err,
						sql: sql,
						msg: 'Error during connection'
					});
				}
				else {
					connection.query(sql, (err, results) => {
						if (err) {
							this.log('Error executing query', err);
							callback({
								err: err,
								sql: sql,
								msg: 'Error during query'
							});
						}
						else {
							callback(null, {
								results: results,
								sql: sql
							});
						}
					});
				}
			});
		}
		catch (ex) {
			callback({
				err: ex,
				sql: sql,
				msg: 'Exception during connection or query '
			});
		}
	}
}

exports.YourSql = YourSql;
