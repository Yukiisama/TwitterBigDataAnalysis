const express = require("express");
const app = express();
const Controller = require("./controller/Controller");
const PORT = process.env.PORT || 5555;

app.use(express.json());

app.get("/", (req, res) => res.send("Hello ple app"));
app.get("/example", Controller.example);

app.listen(PORT, () => {
    console.log(`Projet ple is running on port ${PORT}`);
});



module.exports = app;
