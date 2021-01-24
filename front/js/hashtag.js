let topk;
let day = document.getElementById('day').value;
let size = document.getElementById('size').value;

let table = "hashtagTopK";

$.ajaxSetup({
    async: false
});

builder(table);
AllDaysOnClick();
reload("resetTopk");


function builder(tableName){
    $.getJSON("http://localhost:25559/" + tableName + "/" + day + '/' + size, function (data) {
        topk = data;
        return data;
    });

    const table = document.getElementById("tabHashtag");
    table.innerHTML = "";
    for (let i = 0 ; i < size; i++){

        const tr = document.createElement("tr");
        createEntries(tr, i);
        createEntries(tr, topk[i].count);
        createEntries(tr, topk[i].hashtag);
        table.appendChild(tr);
    }

    reload('Sday');
    reload('Ssize');
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
        builder(table);
    }
}


function reload(name){
    console.log('cc')
    document.getElementById(name).onclick = function() {
        location.reload();
    }
}




