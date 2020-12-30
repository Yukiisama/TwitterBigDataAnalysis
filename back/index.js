const express = require("express");
const app = express();
const Controller = require("./controller/Controller");
const HashtagController = require("./controller/HashtagController");
const PORT = process.env.PORT || 5555;

app.use(express.json());

app.get("/", (req, res) => res.send("Hello ple app"));
app.get("/example", Controller.example);
app.get("/hashtag", HashtagController.example);

app.listen(PORT, () => {
    console.log(`Projet ple is running on port ${PORT}`);
});



module.exports = app;
