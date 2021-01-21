const express = require("express");
const app = express();
// app.use(express.static('public'));
var bodyParser = require('body-parser');
const async = require('async');
var cors = require('cors');


const Controller = require("./controller/Controller");
const HashtagController = require("./controller/HashtagController");
const UserController = require("./controller/UserController");
const PORT = process.env.PORT || 25559;
const path = require('path');
 




//Set view engine to ejs
//app.set("view engine", "ejs");

//Tell Express where we keep our front ejs files
//app.set("views", __dirname + "/views"); 

// CSS/Scripts to be used with ejs
//app.use(express.static(path.join(__dirname, 'views')));

//Use body-parser
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false })); 

app.use(express.json());
// app.use(bodyParser);


//app.get("/", (req, res) => res.render("index"));
app.get("/example", Controller.example);
app.get("/hashtagTopK/:day/:size", HashtagController.hashtagTopK);
app.get("/hashtagTopKAllDays/:day/:size", HashtagController.hashtagTopKAllDays);
app.post("/user/", UserController._callback);

//alexandradss8
/*app.post("/user/", function (req, res) {
    UserController._callback(req, res);

});*/
app.listen(PORT, () => {
    console.log(`Projet ple is running on port ${PORT}`);
});



module.exports = app;
