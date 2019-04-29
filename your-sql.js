/* jshint node:true */ /* global define, escape, unescape */
'use strict';

class YourSql {
	constructor() {
		this.mysql = require('mysql');
		this.pool = null;
	}

	init(config) {
		this.pool = this.mysql.createPool(config);
	}
	
	handleConnectionError(connection, callback) {
	    connection.on('error', (err) => {
			this.log('Error during while connecting to database.', err);
			callback && callback(err);
			return;
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

	createDatabase(database) {
		return new Promise((resolve, reject) => {
			this.pool.getConnection((err, connection) => {
				if (err) {
					this.log('Error in connection to database', err);
					reject(err);
					return;
				}
	
				this.log('connected as id ' + connection.threadId);
	
				connection.query(`SHOW DATABASES LIKE '${database}'`, (err, rows) => {
					if (!err) {
						if (!this.hasResults(rows)) {
							this.log('Begin create schema ' + database);
							connection.query('CREATE SCHEMA ' + database, (err, rows) => {
								connection.release();
								if (err) {
									this.log('Error creating schema ' + database, err);
									reject(err);
								}
								else {
									this.log('Done creating schema - ' + database);
									resolve(rows);
								}
							});
						}
						else {
							reject('Schema already exists: ' + database);
						}
					}
					else {
						connection.release();
						this.log('Error checking for schema: ' + database, err);
						reject(err);
					}
	
				});
	
				this.handleConnectionError(connection);
			});
		});
	}
	
	
	
	createUser(username, host, password) {
		return new Promise((resolve, reject) => {
            this.pool.getConnection((err, connection) => {
				if (err) {
					this.log('Error in connection to database', err);
					reject(err);
				}
	
				this.log('connected as id ' + connection.threadId);
	
				connection.query(`SELECT User, Host FROM mysql.user where User = '${username}' and Host = '${host}'`, (err, rows) => {
					if (err) {
					    connection.release();
						this.log(`Error checking for user: ${username}@${host}`);
						reject(err);
						return;
					}
					if (!this.hasResults(rows)) {
						this.log(`Begin create user: ${username}@${host}`);
						const identifiedBy = password !== undefined && password !== null ? ` IDENTIFIED BY '${password}'` : '';
						connection.query(`CREATE USER '${username}'@'${host}'${identifiedBy}`, (err, rows) => {
							connection.release();
							if (err) {
								this.log(`Error creating user: ${username}@${host}`, err);
								reject(err);
							}
							else {
								this.log(`Done creating user: ${username}@${host}`);
								resolve(rows);
							}
						});
					}
					else {
						reject(`User already exists: ${username}@${host}`);
					}
	
				});
				this.handleConnectionError(connection);
			});
        });
		
	}
	
	
	
	grantAllCrudRightsToUserOnDatabase(username, host, database) {
		return new Promise((resolve, reject) => {
			this.pool.getConnection((err, connection) => {
				if (err) {
					this.log('Error in connection to database', err);
					reject(err);
				}
	
				this.log('connected as id ' + connection.threadId);
	
				connection.query(`SELECT User, Host FROM mysql.user where User = '${username}' and Host = '${host}'`, (err, rows) => {
				    if(err) {
				        connection.release();
						this.log(`Error checking for user: ${username}@${host}`);
						reject(err);
						return;
				    }
				    
					if (!this.hasResults(rows)) {
						reject(`User does not exist exists: ${username}@${host}`);
						return;
					}
					
					connection.query(`SHOW DATABASES LIKE '${database}'`, (err, rows) => {
					    if(err) {
					        connection.release();
	    					this.log('Error checking for schema: ' + database, err);
	    					reject(err);
	    					return;
					    }
					    
						if (!this.hasResults(rows)) {
							reject('Schema does not exist: ' + database);
							return;
						}
						
						const grantStatement = `GRANT SELECT,INSERT,UPDATE,DELETE ON ${database}.* TO '${username}'@'${host}'`;
						connection.query(grantStatement, (err, rows) => {
							connection.release();
							if (err) {
								this.log(`Error granting rights: ${grantStatement}`, err);
								reject(err);
							}
							else {
								this.log(`Rights granted: ${grantStatement}`);
								resolve(rows);
							}
						});
	    
	    			});
	
				});
				this.handleConnectionError(connection);
			});
		});
		
	}
	
	changeUserPassword(username, host, newPassword, callback) {
		return new Promise((resolve, reject) => {
			this.pool.getConnection((err, connection) => {
				if (err) {
					this.log('Error in connection to database', err);
					reject(err);
				}
	
				this.log('connected as id ' + connection.threadId);
	
				connection.query(`SELECT User, Host FROM mysql.user where User = '${username}' and Host = '${host}'`, (err, rows) => {
				    if(err) {
				        connection.release();
						this.log(`Error checking for user: ${username}@${host}`);
						reject(err);
						return;
				    }
				    
					if (!this.hasResults(rows)) {
						reject(`User does not exist exists: ${username}@${host}`);
						return;
					}
					
					connection.query(`SET PASSWORD FOR '${username}'@'${host}' = PASSWORD('${newPassword}')`, (err, rows) => {
						connection.release();
						if (err) {
							this.log(`Set password failed: SET PASSWORD FOR '${username}'@'${host}' `, err);
							reject(err);
						}
						else {
							this.log(`Done setting password: SET PASSWORD FOR '${username}'@'${host}'`);
							resolve(rows);
						}
					});
				});
	
				this.handleConnectionError(connection);
			});
		});
	}


	getSchemaSizeInMb(schema, callback) {
		return new Promise((resolve, reject) => {
			var sizeQuery = 'SELECT Round(Sum(data_length + index_length) / 1024 / 1024, 1) "mb" FROM information_schema.tables WHERE table_schema = "' + schema + '"';
			this.query(sizeQuery, (err, results) => {
				if (err) {
					this.log('Error querying schema size', err);
					reject(err);
				}
				else {
					if (results === undefined || results === null || results.results === undefined || results.results === null || results.results[0] === undefined || results.results[0] === null) {
						reject('No results for schema ' + schema);
					}
					else {
						resolve(results.results[0].mb);
					}
				}
			})
		});
		
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
		if (sql === undefined || sql === null || sql.length < 1) {
			const message = '"sql" paremeter was not provided';
			this.log(message);
			callback({
				err: message
			});
			return;
		}
		else {
			this.log('SQL STATEMENT: ' + sql);
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

module.exports = function() {
	return new YourSql();
}
