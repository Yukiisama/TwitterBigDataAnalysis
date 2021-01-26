const Service = require("../service/Service");
const service = new Service();

class InfluenceursController {

    constructor(service) {
        this.service = service;
        this.tripleHashtagTopK = this.tripleHashtagTopK.bind(this);
        this.influenceursTopK = this.influenceursTopK.bind(this);

    }

    async tripleHashtagTopK(req, res) {
        return await this.service.tripleHashtagsTopK(req.params, res, 'ape-jma_TripleHashtags');
    }

    async influenceursTopK(req, res) {
        return await this.service.influenceursTopK(req.params, res, 'ape-jma_influenceurs');
    }




}

module.exports = new InfluenceursController(service);