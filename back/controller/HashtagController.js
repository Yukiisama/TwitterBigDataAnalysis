const Service = require("./../service/Service");
const service = new Service();

class HashtagsController {

    constructor(service) {
        this.service = service;
        this.hashtagTopK = this.hashtagTopK.bind(this);
        this.hashtagTopKAllDays = this.hashtagTopKAllDays.bind(this);
        this.allHashtags = this.allHashtags.bind(this);
        this.userHashtags = this.userHashtags.bind(this);
    }

    async hashtagTopK(req, res) {
        if (req.params.day < 0)
            req.params.day = 0;
        if (req.params.day > 20)
            req.params.day = 20;
        console.log(req.params);
        return await this.service.hashtagsTopK(req.params, res, 'ape-jma_topKHashtag' + req.params.day);
    }

    async hashtagTopKAllDays(req, res) {
        return await this.service.hashtagsTopK(req.params, res, 'ape-jma_topKHashtagAll');
    }

    async allHashtags(req, res){
        console.log("yo");
        return await this.service.hashtagsTopK(req.params, res, 'ape-jma_Hashtags');
    }

    async userHashtags(req, res){
        return await this.service.userList(req.params, res, 'ape-jma_usersHashtagsList');
    }


}

module.exports = new HashtagsController(service);