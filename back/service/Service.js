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
            if (params.size < 0) 
                params.size = 10000;
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                params.size = Number(params.size);
                if (value.length < params.size)
                    params.size = value.length;
                console.log(value.length, params.size)
                if (value.length >= params.size){
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
     async fakeInfluenceursTopK(params, res, tableName) {
        try {
            // exemple hbase
            if (params.size < 0) 
                params.size = 10000;
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                params.size = Number(params.size);
                if (value.length < params.size)
                    params.size = value.length;
                if (value.length >= params.size){
                    val = [];
                    let nb_messages = [];
                    let influenceurs = [];  
                    let retweets_count = [];
                    for (let i = 0; i < value.length; i++){  
                        if (value[i].column == "global:nb_messages") nb_messages[value[i].key] =  (value[i].$)
                        else if (value[i].column == "global:retweets_count") retweets_count[value[i].key] =  (value[i].$)
                        else influenceurs[value[i].key] =  (value[i].$)
                    } 
                    console.log(params.size);
                    for (let i = 0; i < params.size; i++){  
                        val[i] = {
                            nb_messages: (nb_messages[i]),
                            influenceurs: (influenceurs[i]),
                            retweets_count: (retweets_count[i])
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
     async influenceursTopK(params, res, tableName) {
        try {
            // exemple hbase
            if (params.size < 0) 
                params.size = 10000;
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                params.size = Number(params.size);
                if (value.length < params.size)
                    params.size = value.length;
                if (value.length >= params.size){
                    val = [];
                    let nb_messages = [];
                    let influenceurs = [];  
                    for (let i = 0; i < value.length; i++){  
                        if (value[i].column == "global:nb_messages") nb_messages[value[i].key] =  (value[i].$)
                        else influenceurs[value[i].key] =  (value[i].$)
                    } 
                    console.log(params.size);
                    for (let i = 0; i < params.size; i++){  
                        val[i] = {
                            nb_messages: (nb_messages[i]),
                            influenceurs: (influenceurs[i])
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

    async tripleHashtagsTopK(params, res, tableName) {
        try {
            // exemple hbase
            if (params.size < 0) 
                params.size = 10000;
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                params.size = Number(params.size);
                if (value.length < params.size)
                    params.size = value.length;
                if (value.length >= params.size){
                    val = [];
                    let count = [];
                    let hashtag1 = [];  
                    let hashtag2 = [];  
                    let hashtag3 = [];  
                    for (let i = 0; i < value.length; i++){  
                        if (value[i].column == "global:count") count[value[i].key] =  (value[i].$)
                        else if (value[i].column == "global:hashtag1") hashtag1[value[i].key] =  (value[i].$)
                        else if (value[i].column == "global:hashtag2") hashtag2[value[i].key] =  (value[i].$)
                        else hashtag3[value[i].key] =  (value[i].$)
                    } 
                    console.log(params.size);
                    for (let i = 0; i < params.size; i++){  
                        val[i] = {
                            count: (count[i]),
                            hashtag1: (hashtag1[i]),
                            hashtag2: (hashtag2[i]),
                            hashtag3: (hashtag3[i])
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

    async allTripleHashtag(params, res, tableName) {
        try {
            // exemple hbase
            if (params.size < 0) 
                params.size = 10000;
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                params.size = Number(params.size);
                if (value.length < params.size)
                    params.size = value.length;
                if (value.length >= params.size){
                    val = [];
                    let usernames = [];
                    let hashtag1 = [];  
                    let hashtag2 = [];  
                    let hashtag3 = [];  
                    for (let i = 0; i < value.length; i++){  
                        if (value[i].column == "global:usernames") usernames[value[i].key] =  (value[i].$)
                        else if (value[i].column == "global:hashtag1") hashtag1[value[i].key] =  (value[i].$)
                        else if (value[i].column == "global:hashtag2") hashtag2[value[i].key] =  (value[i].$)
                        else hashtag3[value[i].key] =  (value[i].$)
                    } 
                    console.log(params.size);
                    for (let i = 0; i < params.size; i++){  
                        val[i] = {
                            usernames: (usernames[i]),
                            hashtag1: (hashtag1[i]),
                            hashtag2: (hashtag2[i]),
                            hashtag3: (hashtag3[i])
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

    async userList(params, res, tableName) {
        try {
            const table = hbase.table(tableName);
            table.row('*').get((error, value) => {
                let val = value;
                val = [];
                let username = [];
                let hashtag = [];  
                console.log(value.length);
                for (let i = 0; i < value.length; i++){  
                    
                    if (value[i].column == "global:username") username[value[i].key] =  (value[i].$)
                    else hashtag[value[i].key] =  (value[i].$)
                } 
                for (let i = 0; i < params.size; i++){  
                    val[i] = {
                        username: (username[i]),
                        hashtag: (hashtag[i])
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