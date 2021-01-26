let topk;
let size = document.getElementById('size').value;

let table = "tripleHashtags/";

$.ajaxSetup({
    async: false
});

builder(table);
Influenceurs();
/*AllDaysOnClick();
Hashtags();
userList();*/
reload("resetTopk");

function builder(tableName){
    $.getJSON("http://localhost:25559/" + tableName + size, function (data) {
        topk = data;
        return data;
    });
    document.getElementById('head').innerHTML = "triple hashtag TopK "
    const table = document.getElementById("tabInfluenceurs");
    table.innerHTML = "";
    const key = document.getElementById("secondKeyInf");
    key.innerHTML = "Hashtag1"
    const key3 = document.getElementById("threeKeyInf");
    key3.innerHTML = "Hashtag2"
    const key4 = document.getElementById("fourKeyInf");
    key4.innerHTML = "Hashtag3"
    const key2 = document.getElementById("firstKeyInf");
    key2.innerHTML = "position topK"
    
    for (let i = 0 ; i < size; i++){

        const tr = document.createElement("tr");
        createEntries(tr, i);
        createEntries(tr, topk[i].hashtag1);
        createEntries(tr, topk[i].hashtag2);
        createEntries(tr, topk[i].hashtag3);
        createEntries(tr, topk[i].count);
        table.appendChild(tr);
    }
    reload('Isize');

}

function builderInfluenceurs(tableName){
    $.getJSON("http://localhost:25559/" + tableName + size, function (data) {
        topk = data;
        return data;
    });
    document.getElementById('head').innerHTML = " Top k Influenceurs"
    const table = document.getElementById("tabInfluenceurs");
    table.innerHTML = "";
    const key = document.getElementById("secondKeyInf");
    key.innerHTML = "influenceurs"
    const key3 = document.getElementById("threeKeyInf");
    key3.innerHTML = "nbmessages"
    const key4 = document.getElementById("fourKeyInf");
    key4.innerHTML = ""
    const key2 = document.getElementById("firstKeyInf");
    key2.innerHTML = "position topK"
    console.log(topk);
    for (let i = 0 ; i < size; i++){

        const tr = document.createElement("tr");
        createEntries(tr, i);
        createEntries(tr, topk[i].influenceurs);
        createEntries(tr, topk[i].nb_messages);
        table.appendChild(tr);
    }
    reload('Isize');

}

function createEntries(tr, data) {    
    let v = document.createElement("td");
    v.innerHTML = data;
    tr.appendChild(v);
}


function AllDaysOnClick() {
    const element = document.getElementById("topkDescription");
    const swap = document.getElementById("swapTopk");
    element.innerHTML = "";
    table = "hashtagTopKAllDays"
    swap.onclick = function() {
        builder("hashtagTopKAllDays");
    }
}

function Influenceurs() {
    const all = document.getElementById("allinf");
    table = "influenceurs/"
    all.onclick = function() {
        builderInfluenceurs("influenceurs/");
    }
}

function userList() {
    const element = document.getElementById("topkDescription");
    element.innerHTML = "";
    const all = document.getElementById("userHashtags");
    all.onclick = function() {
        builderUserList();
    }
}



function reload(name){
    console.log('cc')
    document.getElementById(name).onclick = function() {
        location.reload();
    }
}




