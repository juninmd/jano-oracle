const mu = require('./messageUtil');

module.exports = (connection) => {
    return {
        executeString: (rm, query) => {
            return new Promise((resolve, reject) => {
                connection.execute(query, (err, result) => {
                    if (err) {
                        connection.release(() => {
                            return reject(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", err.message));
                        });
                    }
                    else {
                        connection.release(() => {
                            if (result.rows.length == 0) {
                                return resolve({ metaData: [], content: [] });
                            }
                            else {
                                return resolve({ metaData: result, content: result.rows });
                            }
                        });
                    }
                });
            });
        },
        executeProcedure: (rm) => {
            return new Promise((resolve, reject) => {
                connection.execute(`BEGIN ${rm.database.procedure}(${rm.database.parametros.ToParamtersOracle()}) ;END;`, (err, result) => {
                    if (err) {
                        connection.release(() => {
                            return reject(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", err.message));
                        });
                    }
                    else {
                        connection.commit(() => {
                            connection.release(() => {
                                if (result.affectedRows == 1) {
                                    return resolve({ metaData: [], content: { retorno: 'OK', Id: result.insertId } });
                                }
                                else {
                                    return resolve(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", "O registro não foi adicionado."));
                                }
                            });
                        });
                    }
                });
            });
        },
        readProcedure: (rm) => {
            return new Promise((resolve, reject) => {
                connection.execute(`BEGIN ${rm.database.procedure}(${rm.database.parametros.ToParamtersOracle()}) ;END;`, rm.database.parametros, (err, result) => {
                    if (err) {
                        connection.release(() => {
                            return resolve(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", err.message));
                        });
                    }
                    else {
                        connection.release(() => {
                            return resolve({ content: result[0] });
                        });
                    }
                });
            });
        },
        executeObject: (rm, table, object, ) => {
            return new Promise((resolve, reject) => {
                connection.execute(table, object, (err, info) => {
                    if (err) {
                        connection.release(() => {
                            return reject(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", err.message));
                        });
                    }
                    else {
                        connection.release(() => {
                            if (info.affectedRows == 0) {
                                return resolve(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", "Registro não foi inserido."));
                            }
                            else {
                                return resolve({ metaData: [], content: info.insertId });
                            }
                        })
                    }
                });
            });
        },
        executeTransaction: (rm, table, object) => {
            return new Promise((resolve, reject) => {
                connection.execute(table, object, (err, info) => {
                    if (err) {
                        return resolve(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", err.message));
                    }
                    else {
                        if (info.affectedRows == 0) {
                            return resolve(mu.setError(rm, 500, "Ocorreu um problema com essa operação, tente novamente.", "Registro não foi inserido."));
                        }
                        else {
                            return resolve({ metaData: [], content: info.insertId });
                        }
                    }
                });
            });
        },
        connection: () => {
            return connection;
        }
    }
};
Object.prototype.ToParamtersOracle = function () {
    return Object.getOwnPropertyNames(this).map(x => `:${x} `).toString();
};

function fetchRows(connection, resultSet, rm, callback) {
    resultSet.getRow((err, row) => {
        if (err) {
            connection.release(() => {
                callback(mu.setError(rm, 500, "Ocorreu uma falha no banco de dados.", err.message), null);
            });
        } else if (!row) {
            connection.release(() => {
                callback(null, rm.content);
            });
        } else {
            rm.content.push(row);
            fetchRows(connection, resultSet, response, callback);
        }
    });
}