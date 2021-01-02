const Service = require("./../service/Service");
const service = new Service();

class UserController {

    constructor(service) {
        this.service = service;
        this._callback = this.user.bind(this);
    }


    async user(req, res) {
        const response = await this.service.user(req.params);
        

        console.log(response.data);

        const regex_replace_$ = /('\$')/gi;

        let raw = JSON.stringify(response.data);
        raw = raw.replace("'$'", '"value"');
        raw = raw.replace("'", '"');

        // console.log(raw);
        var data = JSON.parse(raw);

        try{
            let name = data[0].$;
            let received_favs = data[1].$;
            let received_rts = data[2].$;
            let tweets_posted = data[3].$;
            let daily_frequencies = data[4].$;
            let hashtags = data[5].$;
            let langs = data[6].$
                .replace('{', "")
                .replace("}", '')
                .replace(" ", '')
                .split(",");
            console.log("Langue:");
            console.log(langs);

            let parsed_langs = {};
            
            for( let i = 0; i < langs.length; i++ ) {
                let lang = langs[i];
                let tmp = lang.split("=");

                parsed_langs[langs[i].substring(0,2)] = tmp[1];
            }

            let history_localisation = data[7].$;
            let history_sources = data[8].$;

            console.log(parsed_langs);
            

            langs = {};
            // for( let i = 0; i < parsed_langs.length; i++ ) {
            //     langs[k] = {label : k, value: parsed_langs[k]}
            // }

            langs = Object.keys(parsed_langs).map(function(k) {
                return {
                  label: k,
                  value: parsed_langs[k]
                };
              });
              

            console.log(langs);


            return res.render("pages/users", {
                name: name,
                favs : received_favs,
                rts: received_rts,
                tweets: tweets_posted,
                frequencies: daily_frequencies,
                hashtags: hashtags,
                langs: langs,
                localisations: history_localisation,
                sources: history_sources
            });
        } catch(error) {
            console.log(error);
            console.log("Actualisez la page.");
        }

    }
// Remove trailing [ and ]
// /(^\[ | \]$)/ig 


// [[][[:blank:]]*[{] ((column|timestamp|$): ('*'[a-z]*:[a-z]*|[0-9]*)',[[:cntrl:]]*[[:blank:]]*)*
}


module.exports = new UserController(service);