let topk;
let day = document.getElementById('day').value;
let size = document.getElementById('size').value;

let table = "hashtagTopK";

$.ajaxSetup({
    async: false
});

builder(table, document.getElementById('size').value, " User list with hashtags");
AllDaysOnClick();
Hashtags();
userList();
reload("resetTopk");

function builder(tableName, size, name){
    $.getJSON("http://localhost:25559/" + tableName + "/" + day + '/' + size, function (data) {
        topk = data;
        return data;
    });
    document.getElementById("head1").innerHTML = name;
    const table = document.getElementById("tabHashtag");
    table.innerHTML = "";
    const key = document.getElementById("secondKey");
    key.innerHTML = "Count"
    const key2 = document.getElementById("firstKey");
    key2.innerHTML = "TopK Position"
    for (let i = 0 ; i < size; i++){

        if(topk[i]) {
            const tr = document.createElement("tr");
            createEntries(tr, i);
            createEntries(tr, topk[i].count);
            createEntries(tr, topk[i].hashtag);
            table.appendChild(tr);
        }
    }

    reload('Sday');
    reload('Ssize');
}

function builderUserList(){
    $.getJSON("http://localhost:25559/" + "userHashtags/" + size, function (data) {
        topk = data;
        return data;
    });
    document.getElementById("head1").innerHTML = " User list with hashtags";
    const table = document.getElementById("tabHashtag");
    table.innerHTML = "";
    const key = document.getElementById("secondKey");
    key.innerHTML = "Username"
    const key2 = document.getElementById("firstKey");
    key2.innerHTML = "position"

    for (let i = 0 ; i < size; i++){

        const tr = document.createElement("tr");
        createEntries(tr, i);
        createEntries(tr, topk[i].username);
        createEntries(tr, topk[i].hashtag);
        table.appendChild(tr);
    }

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
        builder("hashtagTopKAllDays", document.getElementById('size').value, " Hashtags Topk all days");
    }
}

function Hashtags() {
    const element = document.getElementById("topkDescription");
    element.innerHTML = "";
    const all = document.getElementById("allHashs");
    table = "hashtags"
    all.onclick = function() {
        builder("hashtags", document.getElementById('size').value, " All hashtags");
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




