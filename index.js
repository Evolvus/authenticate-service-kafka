var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    Consumer = kafka.Consumer,
    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client);

console.log('*********Authentication Node Service(Processor)*********');

var userList = ["shrimank", "anisht", "kumaresan"];
// var credentials = {
//     userName: 'shrimank',
//     userPassword: 'evolvus*123'
// };




consumer = new Consumer(
    client, [
        { topic: 'pendingAuthenticationQ', partition: 0 }
    ], {
        autoCommit: true
    }
);
consumer.on('message', function(message) {
    console.log("Consumed ", message);





    var credentials = JSON.parse(message.value);
    console.log(credentials.userName);
    credentials.authenticated = false;
    userList.forEach(u => {
        if (u === credentials.userName) {
            credentials.authenticated = true;
        }

    });
    var km = new KeyedMessage('key', JSON.stringify(credentials));
    console.log("__________________________")

    var payloads = [
        { topic: 'AuthenticatedQ', messages: km, partition: 0 }
    ];
    producer.send(payloads, function(err, data) {
        console.log("Produced after authentication", data);
        console.log("__________________________");

    });


    producer.on('error', function(err) {
        console.log("Error", err);
    })


});