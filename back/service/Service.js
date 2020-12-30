const hbase = require('hbase')();

class Service {
    constructor() {
    }
    
    async example(params){
        try {
            // exemple hbase
            const table = hbase.table( 'rnavarro');
            const example = table.schema(function(error, schema){
                console.info(schema)
            });
            console.log("it works");
            return {code: 200, data: {note: "le json qu'on voudra retourner"}, error: false};
        } 
        catch (error) {
            return {code: 400, error: error};
        }

    }



    
    async hashtags(params){
        try {
            // exemple hbase
            const table = hbase.table( 'rnavarro');
            const example = table.schema(function(error, schema){
                console.info(schema)
            });
            console.log("it works");
            return {code: 200, data: {note: "le json qu'on voudra retourner"}, error: false};
        } 
        catch (error) {
            return {code: 400, error: error};
        }

    }
}

module.exports = Service;