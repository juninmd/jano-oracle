const mu = require('./messageUtil');
const oracle = require('oracledb');
let CONNECTIONSTRING = {};
module.exports = (connectionString) => {
    CONNECTIONSTRING = connectionString;
    return {
        executeString: async (query) => {
            let newMu = mu.new(200, null, null, null, null, null);
            let connection = await OracleInit();
            return await require("./coreOracle.js")(connection).executeString(newMu, query);
        },
        executeObject: async (query, object) => {
            let newMu = mu.new(200, null, null, null, query, null);
            let connection = await OracleInit();
            return require("./coreOracle.js")(connection).executeObject(newMu, query, object)
        },
        executeProcedure: async (procedure, array) => {
            let newMu = mu.new(200, null, null, array, null, procedure);
            let connection = await OracleInit();
            return require("./coreOracle.js")(connection).executeProcedure(newMu);
        },
        readProcedure: async (procedure, array) => {
            let newMu = mu.new(200, null, null, array, null, procedure);
            let connection = await OracleInit();
            return require("./coreOracle.js")(connection).readProcedure(newMu, array);
        },
        beginTransaction: async () => {
            let connection = await OracleInit();
            return new Promise((resolve, reject) => {
                return connection.beginTransaction((err) => {
                    return err ? reject(err) : resolve(connection);
                });
            });
        },
        executeTransaction: async (connection, query, object) => {
            return new Promise((resolve, reject) => {
                let newmu = mu.new(200, '', '', object, query);
                return require("./coreOracle.js")(connection).executeTransaction(newMu, query, object);
            });
        }
    }
}

let OracleInit = () => {
    return new Promise((resolve, reject) => {
        let connection = {};
        return oracle.getConnection(CONNECTIONSTRING)
            .then(q => {
                connection = q;
                connection.__proto__.endConnection = endConnection;
                return resolve(connection);
            }).catch(err => {
                return reject(err);
            })
    })
}

function endConnection(commit) {
    if (commit == undefined)
        commit = true;
    let _self = this;
    if (commit) {
        return new Promise((resolve, reject) => {
            _self.commit(() => {
                _self.release(() => {
                    return resolve({ 'conexão': 'encerrada', status: 'comitado' });
                });
            });
        })
    }
    else {
        return new Promise((resolve, reject) => {
            _self.rollback(() => {
                _self.release(() => {
                    return resolve({ 'conexão': 'encerrada', status: 'rollback' });
                });
            });
        })
    }
};