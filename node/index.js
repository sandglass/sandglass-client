var sgproto = require('@sandglass/grpc');
var grpc = require('grpc')
var client = new sgproto.BrokerService(':7170', grpc.credentials.createInsecure());

client.getTopic({
    name: "emails"
}, function(err, resp) {
    if (err) {
        console.log(err)
        return;
    }
    console.log(resp)
})
