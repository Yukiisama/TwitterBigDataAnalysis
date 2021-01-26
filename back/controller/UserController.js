var tags = require('language-tags')

const Service = require("./../service/Service");
const service = new Service();

class UserController {

    constructor(service) {
        this.service = service;
        this._callback = this.user.bind(this);
    }
    
    async user(req, res) {
        return await this.service.user(req.body.search, this, res);
    }

    parseAndReplace(data){
        return JSON.parse(JSON.stringify(data).replace(/&#34;/g,'"'));
    }

    parseInputResponse(response) {
        const regex_replace_$ = /('\$')/gi;

        let raw = JSON.stringify(response);
        raw = raw.replace("'$'", '"value"');
        raw = raw.replace("'", '"');
        return JSON.parse(raw);
    }
    // Remove trailing [ and ]
    // /(^\[ | \]$)/ig 
    // [[][[:blank:]]*[{] ((column|timestamp|$): ('*'[a-z]*:[a-z]*|[0-9]*)',[[:cntrl:]]*[[:blank:]]*)*



    getLangs(raw_lang) {
        let langs = raw_lang
            .replace('{', "")
            .replace("}", '')
            .split(", ");

        let parsed_langs = {};
            
        for( let i = 0; i < langs.length; i++ ) {
            let lang = langs[i];
            let tmp = lang.split("=");

            parsed_langs[langs[i].substring(0,2)] = tmp[1];
        }


        langs = {};

        return langs = Object.keys(parsed_langs).map(function(k) {

            let name = "Inconnu.";
            try{
                name = tags.language(k).descriptions();
            } catch (error) {

            }

            if(!name.length == 0) {
                name = name[0];
                if(k.includes("fr")){
                    name = "Français"; // Turn "French" to "Français"
                }
            }

            if(!name.length == 0) {
                name = name[0];
                if(k.includes("und")){ // Undefined language tag: https://tools.ietf.org/html/bcp47
                    name = "Inconnu";
                }
            }

            

            return {
                label: name,
                value: parsed_langs[k]
            };
        });
        
    }


    getHashtags(raw_hashtags) {
        let hashtags = raw_hashtags
            .replace('{', "")
            .replace("}", '')
            .split(", ");

        let parsed_hashtags = {};

            
        for( let i = 0; i < hashtags.length; i++ ) {

            console.log("Value raw: " + hashtags[i]);
            let hashtag = hashtags[i];
            let tmp = hashtag.split("=");
            parsed_hashtags[tmp[0]] = tmp[1];

        }


        hashtags = {};
        hashtags = Object.keys(parsed_hashtags).map(function(k) {
            return {
                label: k,
                value: parsed_hashtags[k]
            };
        }).sort((a, b) => (a.value < b.value) ? 1 : -1);

        return hashtags;
        
    }



    getDateStatistics(raw_dates) {
        let dates = raw_dates
            .replace('{', "")
            .replace("}", '')
            .split(",");
        let parsed_dates = {};
            
        for( let i = 0; i < 24; i++ ) {
            if(!(dates[i] === undefined)) {

                let date = dates[i];
                let tmp = date.split("=");
    
                parsed_dates[i + "h"] = tmp[1];

            } else {
                parsed_dates[i + "h"] = 0;
            }


        }


        dates = {};
        return dates = Object.keys(parsed_dates).map(function(k) {
            return {
                label: k,
                value: parsed_dates[k]
            };
        });
    }

}


module.exports = new UserController(service);