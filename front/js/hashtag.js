let topk;
let day = document.getElementById('day').value;
let size = document.getElementById('size').value;

$.ajaxSetup({
    async: false
});
$.getJSON("http://localhost:25559/hashtagTopK/" + day + '/' + size, function (data) {
    topk = data;
    return data;
});

const table = document.getElementById("tabHashtag");
for (let i = 0 ; i < size; i++){

    const tr = document.createElement("tr");
    createEntries(tr, i);
    createEntries(tr, topk[i].count);
    createEntries(tr, topk[i].hashtag);
    table.appendChild(tr);
}

reload('Sday');
reload('Ssize');

function createEntries(tr, data) {    
    let v = document.createElement("td");
    v.innerHTML = data;
    tr.appendChild(v);
}

function reload(name){
    document.getElementById(name).onclick = function() {
        location.reload();
    }
}