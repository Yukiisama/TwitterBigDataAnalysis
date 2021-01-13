const Service = require("./../service/Service");
const service = new Service();

class HashtagsController {

    constructor(service) {
        this.service = service;
        this.hashtagTopK = this.hashtagTopK.bind(this);
    }

    async hashtagTopK(req, res) {
        return await this.service.hashtagsTopK(req.params, res);
    }

}

module.exports = new HashtagsController(service);