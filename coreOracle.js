var rmUtil = require('../../../helpers/requestMessageUtil.js');
var oracledb = require('oracledb');

module.exports = function (connection) {
    var modules = {};

    modules.beginProcedure = function (rm, procedure, bindvars, cursorName, callback) {
        beginProcedure(rm, procedure, bindvars, cursorName, callback);
    };
    modules.beginProcedureById = function (rm, procedure, bindvars, callback) {
        beginProcedureById(rm, procedure, bindvars, callback);
    };
    modules.beginProcedureByIdNew = function (rm, options, bindvars, callback) {
        beginProcedureByIdNew(rm, options, bindvars, callback);
    };
    modules.executeProcedure = function (rm, procedure, bindvars, resultParamter, callback) {
        executeProcedure(rm, procedure, bindvars, resultParamter, callback);
    };
    modules.executeString = function (rm, query, callback) {
        executeString(rm, query, callback);
    };
    conn = connection;
    return modules;
};

var conn = "";
/**
 * Método feito para executar algum SELECT
 */
function beginProcedure(rm, procedure, bindvars, cursorName, callback) {
    rm.content = [];
    conn.execute(`BEGIN ${procedure}(${bindvars.ToParamtersOracle()}) ;END;`,
        bindvars,
        function (err, result) {
            if (err) {
                callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                return;
            }
            var metaData = result.outBinds[cursorName].metaData.map(e => e.name);
            fetchRows(result.outBinds[cursorName], rm, function (err, result) {
                if (err) {
                    callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                    return;
                }
                callback(null, { metaData: metaData, content: result });
                return;
            });

        });
}

function beginProcedureById(rm, procedure, bindvars, callback) {
    conn.execute(`BEGIN ${procedure}(${bindvars.ToParamtersOracle()}) ;END;`,
        bindvars,
        function (err, result) {
            if (err) {
                callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                return;
            }
            var metaData = result.outBinds[bindvars.ToParamtersOracle().split(",")[0].replace(":", "").trim()].metaData.map(e => e.name);
            result.outBinds[bindvars.ToParamtersOracle().split(",")[0].replace(":", "").trim()].getRow(
                function (err, resposta) {
                    if (err) {
                        conn.release(function () {
                            callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                        });
                    } else if (!resposta) {
                        conn.release(function () {
                            callback(rmUtil.returnError(rm, 404, "O registro não foi encontrado.", ""), null);
                        });
                    } else
                        conn.release(function () {
                            callback(null, { metaData: metaData, content: resposta });
                        });
                });
        });
}

function beginProcedureByIdNew(rm, options, bindvars, callback) {
    conn.execute(`BEGIN ${options.procedure}(${bindvars.ToParamtersOracle()}) ;END;`,
        bindvars,
        function (err, result) {
            if (err) {
                callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                return;
            }
            var metaData = result.outBinds[bindvars.ToParamtersOracle().split(",")[0].replace(":", "").trim()].metaData.map(e => e.name);
            result.outBinds[bindvars.ToParamtersOracle().split(",")[0].replace(":", "").trim()].getRow(
                function (err, resposta) {
                    if (err) {
                        conn.release(function () {
                            callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                        });
                    } else if (!resposta) {
                        conn.release(function () {
                            callback(rmUtil.returnError(rm, 404, "O registro não foi encontrado.", ""), null);
                        });
                    } else
                        if (!options.close) {
                            callback(null, { metaData: metaData, content: resposta });
                        }
                        else {
                            conn.release(function () {
                                callback(null, { metaData: metaData, content: resposta });
                            });
                        }
                });
        });
}

function fetchRows(resultSet, requestMessage, callback) {
    resultSet.getRow(
        function (err, row) {
            if (err) {
                conn.release(function () {
                    callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao obter os dados.", err.message), null);
                });
            } else if (!row) {
                conn.commit(function () {
                    conn.release(function () {
                        callback(null, requestMessage.content);
                    });
                });
            } else {
                requestMessage.content.push(row);
                fetchRows(resultSet, requestMessage, callback);
            }
        });
}

/**
 * 
 * resultParamter : P_RESULT ?
 */
function executeProcedure(rm, procedure, bindvars, resultParamter, callback) {
    conn.execute(`BEGIN ${procedure}(${bindvars.ToParamtersOracle()}) ;END;`,
        bindvars,
        function (err, result) {
            if (err) {
                conn.release(function () {
                    callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao submeter os dados.", err.message), null);
                });
            }
            else if (result.outBinds[resultParamter] !== null && result.outBinds[resultParamter].startsWith("ORA")) {
                conn.release(function () {
                    callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao submeter os dados.", result.outBinds[resultParamter]), null);
                });
            }
            else {
                conn.commit(function () {
                    conn.release(function () {
                        callback(null, { metaData: [], content: result.outBinds[resultParamter] });

                    });
                });
            }
        }
    );
}

function executeString(rm, query, callback) {
    conn.execute(query,
        [], {},
        function (err, result) {
            if (err) {
                conn.release(function () {
                    callback(rmUtil.returnError(rm, 500, "Ocorreu uma falha ao submeter os dados.", err.message), null);
                });
            }
            else {
                if (result.rows.length == 0) {
                    conn.release(function () {
                        callback(null, { metaData: [], content: [] });
                        return;
                    });
                }

                conn.release(function () {
                    callback(null, { metaData: result.metaData, content: result.rows[0] });
                });
            }
        }
    );
}



Object.prototype.ToParamtersOracle = function () {
    return Object.getOwnPropertyNames(this).map(x => `:${x} `).toString();
};