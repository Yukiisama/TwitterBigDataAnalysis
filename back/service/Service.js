const async = require('async');
const hbase = require('hbase')()
// const client = new hbase.Client({
//   host: 'data',
//   port: 8085
// })


class Service {
    constructor() {
        this.data_user = "";
    }
    
    async example(params){
        try {
            // exemple hbase
            const table = hbase.table( 'rnavarro');
            const example = table.schema(function(error, schema){
                console.info(schema)
                console.info(error)
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



    async getUserDataFromHBase( uuid, _callback) {
        const table = hbase.table( 'ape-jma_users' );
        let res;

        try {
            table.row(uuid).get((error, value) => {
                this.data_user = value;
    
                // console.log(value);
                _callback();
                return value;
            });
        } catch (error) {
            return error;
        }
    }
    
    async user(params){
        let data;
        try {
            data =  this.getUserDataFromHBase("1114690336209686529", function () {
                console.log("Data fetched");
            });
        } catch (error) {
            return {code: 400, error: error};
        }



        return {code: 200, data: this.data_user, error: false};
    }


    convertDataToHTML(data) {
        
    }
}

module.exports = Service;