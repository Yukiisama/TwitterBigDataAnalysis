var tags = require('language-tags')

const Service = require("./../service/Service");
const service = new Service();

class UserController {

    constructor(service) {
        this.service = service;
        this._callback = this.user.bind(this);
    }





    async user(req, res) {
        // console.log("ID: " + req.body.search);


        const response = await this.service.user(req.body.search)
        var data = this.parseInputResponse(response);

        // console.log("Data: " + JSON.stringify(data));

        try{
            let name = data[0].$;
            let received_favs = data[1].$;
            let received_rts = data[2].$;
            let tweets_posted = data[3].$;
            let daily_frequencies = this.getDateStatistics(data[4].$
                .replace('{', "")
                .replace("}", '')
                .split(","));
            let hashtags = data[5].$;
            console.log("Raw Values: " + data[6].$);

            let history_localisation = data[7].$;
            let history_sources = data[8].$;
            let langs = this.getLangs(data[6].$);
                

            // console.log("Dates: " + JSON.stringify(daily_frequencies));
            // console.log(JSON.stringify(langs));


            return res.render("pages/users", {
                name: name,
                favs : received_favs,
                rts: received_rts,
                tweets: tweets_posted,
                frequencies: JSON.stringify(daily_frequencies),
                hashtags: hashtags,
                langs: JSON.stringify(langs),
                localisations: history_localisation,
                sources: history_sources
            });
        } catch(error) {
            console.log(error);
            console.log("Actualisez la page.");
        }

    }



    parseInputResponse(response) {
        const regex_replace_$ = /('\$')/gi;

        let raw = JSON.stringify(response.data);
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
        console.log("Values: " + langs);
            
        for( let i = 0; i < langs.length; i++ ) {
            let lang = langs[i];
            console.log("Value: " + lang);
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
                    name = "Français";
                }
            }

            

            return {
                label: name,
                value: parsed_langs[k]
            };
        });
        
    }



    getDateStatistics(raw_dates) {
        let dates = raw_dates;
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