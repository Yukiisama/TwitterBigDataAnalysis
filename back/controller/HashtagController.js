const Service = require("./../service/Service");
const service = new Service();

class HashtagsController {

    constructor(service) {
        this.service = service;
        this.hashtagTopK = this.hashtagTopK.bind(this);
        this.hashtagTopKAllDays = this.hashtagTopKAllDays.bind(this);
    }

    async hashtagTopK(req, res) {
        return await this.service.hashtagsTopK(req.params, res, 'ape-jma_topKHashtag');
    }

    async hashtagTopKAllDays(req, res) {
        return await this.service.hashtagsTopK(req.params, res, 'ape-jma_topKHashtagAll');
    }

}

module.exports = new HashtagsController(service);