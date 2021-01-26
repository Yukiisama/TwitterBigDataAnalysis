const Service = require("../service/Service");
const service = new Service();

class InfluenceursController {

    constructor(service) {
        this.service = service;
        this.tripleHashtagTopK = this.tripleHashtagTopK.bind(this);
        this.influenceursTopK = this.influenceursTopK.bind(this);
        this.allTripleHashtag = this.allTripleHashtag.bind(this);
        this.fakeInfluenceursTopK = this.fakeInfluenceursTopK.bind(this);

    }

    async tripleHashtagTopK(req, res) {
        return await this.service.tripleHashtagsTopK(req.params, res, 'ape-jma_TripleHashtags');
    }

    async allTripleHashtag(req, res) {
        return await this.service.allTripleHashtag(req.params, res, 'ape-jma_AllTripleHashtags');
    }

    async influenceursTopK(req, res) {
        return await this.service.influenceursTopK(req.params, res, 'ape-jma_influenceurs');
    }

    async fakeInfluenceursTopK(req, res) {
        return await this.service.fakeInfluenceursTopK(req.params, res, 'ape-jma_FakeInfluenceurs');
    }




}

module.exports = new InfluenceursController(service);