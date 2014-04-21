db = db.getSiblingDB('dating_site')

var map = function() {
    var message = this.msg;

    if (message) {

        message = message.split(" ");

        for (var i = message.length - 1; i >= 0; i--) {

            if ( message[i] )  {
               emit(message[i], 1);
            }
        }
    }
};

var reduce = function( key, values ) {
    var count = 0;
    values.forEach(function(v) {
        count +=v;
    });
    return count;
}
/* result */
result = db.messages.mapReduce(map, reduce, {out: "word_count"})

/* to explicity print something out, use printjson */
printjson(result)
