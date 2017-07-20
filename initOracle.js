const oracleDB = require("oracledb");
const OracleConnection = {};

module.exports = {
    beginProcedure: function (databaseName, procedure, bindvars, cursorName, callback) {
        beginProcedure(databaseName, procedure, bindvars, cursorName, callback);
    },
    beginProcedureById: function (databaseName, procedure, bindvars, callback) {
        beginProcedureById(databaseName, procedure, bindvars, callback);
    },
    /**
    ** options: close | procedure | database
    */
    beginProcedureByIdNew: function (options, bindvars, callback) {
        beginProcedureByIdNew(options, bindvars, callback);
    },
    executeProcedure: function (databaseName, procedure, bindvars, resultParamter, callback) {
        executeProcedure(databaseName, procedure, bindvars, resultParamter, callback);
    },
    type: function (type) {
        return oracleDB[type];
    },
    executeString: function (databaseName, query, callback) {
        executeString(databaseName, query, callback);
    },
    readBlob: function (lob, string64, callback) {
        readBlob(lob, string64, callback);
    },
    getConnection: function () {
        return OracleConnection;
    }

};



/** Funcionalidade:
 * Função private responsável pela conexão ao banco de dados
 * com ela fazemos a devida tratativa da conexão.
 * 
 * Paramêtros:
 * Nome do Database | Callback
 */
function OracleInit(databaseName, callback) {
    try {
        oracleDB.outFormat = oracleDB.OBJECT;
        oracleDB.getConnection(webconfig.dataConfig[databaseName],
            function (err, connection) {
                if (err) {
                    callback(requestMessage(500, 'Não foi possível conectar a base de dados', err.message, null, null), null);
                } else {
                    OracleConnection = connection;
                    var coreOracle = require("./coreOracle.js")(connection);
                    callback(null, coreOracle);
                }
            }
        );
    } catch (ex) {
        callback(requestMessage(500, 'Não foi possível conectar a base de dados', `${ex.message} | Database : ${databaseName}`, null));
    }
}

/**
 * Funcionalidade:
 * Espera receber um cursor, apropriado para selects
 * 
 * Paramêtros:
 * Nome da base de Dados | Nome da Procedure | Paramêtros procedures | Nome do Cursor | Callback 
 */
function beginProcedure(databaseName, procedure, bindvars, cursorName, callback) {
    var rm = requestMessage(200, '', '', '', procedure);

    OracleInit(databaseName, function (err, coreOracle) {
        if (err) {
            callback(err);
            return;
        }
        coreOracle.beginProcedure(rm, procedure, bindvars, cursorName, callback);
    });
}

/**
 * Funcionalidade:
 * Espera receber um cursor, apropriado para selects por ID
 * 
 * Paramêtros:
 * Nome da base de Dados | Nome da Procedure | Paramêtros procedures | Callback 
 */
function beginProcedureById(databaseName, procedure, bindvars, callback) {

    var rm = requestMessage(200, '', '', '', procedure);
    OracleInit(databaseName, function (err, coreOracle) {
        if (err) {
            callback(err);
            return;
        }
        coreOracle.beginProcedureById(rm, procedure, bindvars, callback);
    });
}

/**
 * Funcionalidade:
 * Espera receber um cursor, apropriado para selects por ID
 * 
 * Paramêtros:
 * Nome da base de Dados | Nome da Procedure | Paramêtros procedures | Callback 
 */
function beginProcedureByIdNew(options, bindvars, callback) {

    var rm = requestMessage(200, '', '', '', options.procedure);
    OracleInit(options.database, function (err, coreOracle) {
        if (err) {
            callback(err);
            return;
        }
        coreOracle.beginProcedureByIdNew(rm, options, bindvars, callback);
    });
}

/**
 * Funcionalidade:
 * Espera receber um parametro varchar, apropriado para updates/inserts
 * 
 * Paramêtros:
 * Nome da base de Dados | Nome da Procedure | Paramêtros procedures | Callback 
 */
function executeProcedure(databaseName, procedure, bindvars, resultParamter, callback) {

    var rm = requestMessage(200, '', '', '', procedure);
    OracleInit(databaseName, function (err, coreOracle) {
        if (err) {
            callback(err);
            return;
        }
        coreOracle.executeProcedure(rm, procedure, bindvars, resultParamter, callback);
    });
}

function executeString(databaseName, query, callback) {
    var rm = requestMessage(200, '', '', '', query);
    OracleInit(databaseName, function (err, coreOracle) {
        if (err) {
            callback(err);
            return;
        }
        coreOracle.executeString(rm, query, callback);
    });
}

function readBlob(lob, string64, callback) {
    var arrayBytes = [];

    lob.on('close', function () {
        if (!string64) {
            callback(null, arrayBytes);
            return;
        }

        callback(null, "data:image/png;base64," + new Buffer(arrayBytes).toString('base64'));
    });

    lob.on('error', function (err) {
        callback(err, null);
    });

    lob.on('data', function (chunk) {
        Array.prototype.push.apply(arrayBytes, chunk);
    });
}