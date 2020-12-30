const Service = require("./../service/Service");
const service = new Service();

class HashtagsController {

    constructor(service) {
        this.service = service;
        this.example = this.hashtag.bind(this);
    }

    async hashtag(req, res) {
        const response = await this.service.hashtags(req.params);
        return res.status(response.code).send(response);
    }

}

module.exports = new HashtagsController(service);