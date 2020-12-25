const Service = require("./../service/Service");
const service = new Service();

class Controller {

    constructor(service) {
        this.service = service;
        this.example = this.example.bind(this);
    }

    async example(req, res) {
        const response = await this.service.example(req.params);
        return res.status(response.code).send(response);
    }

}

module.exports = new Controller(service);