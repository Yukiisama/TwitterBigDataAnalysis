const express = require("express");
const app = express();
// app.use(express.static('public'));
var bodyParser = require('body-parser');
const async = require('async');


const Controller = require("./controller/Controller");
const HashtagController = require("./controller/HashtagController");
const UserController = require("./controller/UserController");
const PORT = process.env.PORT || 25559;
const path = require('path');
 


app.use(express.json());

//Set view engine to ejs
app.set("view engine", "ejs");

//Tell Express where we keep our front ejs files
app.set("views", __dirname + "/views"); 

// CSS/Scripts to be used with ejs
app.use(express.static(path.join(__dirname, 'views')));

//Use body-parser
app.use(bodyParser.urlencoded({ extended: false })); 

app.get("/", (req, res) => res.render("index"));
app.get("/example", Controller.example);
app.get("/hashtag", HashtagController.example);


app.get("/user", UserController._callback);

app.listen(PORT, () => {
    console.log(`Projet ple is running on port ${PORT}`);
});



module.exports = app;
