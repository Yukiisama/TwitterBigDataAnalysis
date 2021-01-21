const async = require('async');
const { param } = require('..');
const hbase = require('hbase')()
// const client = new hbase.Client({
//   host: 'data',
//   port: 8085
// })


class Service {
    constructor() {
        this.data_user = "";
    }

    async hashtagsTopK(params, res, tableName) {
        try {
            // exemple hbase
            if (params.size < 0 || params.size > 10000) 
                params.size = 10000;
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                
                if (value.length > params.size){
                    val = [];
                    let count = [];
                    let hashtag = [];  
                    for (let i = 0; i < value.length; i++){  
                        /* val[i / 2] = {
                            count: (value[i].$),
                            hashtag: (value[i+1].$)
                        } */

                        if (value[i].column == "global:count") count[value[i].key] =  (value[i].$)
                        else hashtag[value[i].key] =  (value[i].$)
                    } 
                    console.log(params.size);
                    for (let i = 0; i < params.size; i++){  
                        val[i] = {
                            count: (count[i]),
                            hashtag: (hashtag[i])
                        }
                    } 

                }
                return res.status(200).send(val);
            });
        }
        catch (error) {
            return { code: 400, error: error };
        }

    }



    async getUserDataFromHBase(uuid, _this, res, _callback) {
        const table = hbase.table('ape-jma_users');

        try {
            table.row(uuid).get((error, value) => {
                var data = _this.parseInputResponse(value);

                //console.log("Data: " + JSON.stringify(data));
                let name = data[0].$;
                let received_favs = data[1].$;
                let received_rts = data[2].$;
                let tweets_posted = data[3].$;
                let daily_frequencies = _this.getDateStatistics(data[4].$);
                let hashtags = _this.getHashtags(data[5].$);
                console.log("Raw Values: " + data[6].$);

                let history_localisation = data[7].$;
                let history_sources = data[8].$;
                let langs = _this.getLangs(data[6].$);


                // console.log("Dates: " + JSON.stringify(daily_frequencies));
                // console.log(JSON.stringify(langs));

                console.log(data);
                return res.status(200).send({
                    name: name,
                    favs: received_favs,
                    rts: received_rts,
                    tweets: tweets_posted,
                    frequencies: _this.parseAndReplace(daily_frequencies),
                    hashtags: _this.parseAndReplace(hashtags),
                    langs: _this.parseAndReplace(langs),
                    localisations: history_localisation,
                    sources: history_sources
                });
            })
        } catch (error) {
            return error;
        }
    }


    async user(params, _this, res) {
        let data;
        let start = Date.now()
        console.log(params);
        try {
            // data =  this.getUserDataFromHBase(params, function () {
            data = await this.getUserDataFromHBase(params, _this, res, function () {
                let end = Date.now()
                console.log(`User HBase Parsing Time: ${end - start} ms`);

            });
            return { code: 200, data: this.data_user, error: false };
        } catch (error) {
            return { code: 400, error: error };
        }



    }


    convertDataToHTML(data) {

    }
}

module.exports = Service;