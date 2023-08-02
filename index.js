const app = require('express')()
const Kafka = require('node-rdkafka');
const PORT = 3000;
app.listen(PORT, () => { console.log("Server Listening started at port ::" + PORT); })

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, {
    topic: 'test'
});

stream.on('error', (err) => {
    console.error('Error in our kafka stream');
    console.error(err);
});

function queueRequest(req) {
    let event = JSON.stringify(req)
    const success = stream.write(Buffer.from(event));
    if (success) {
        console.log(`message queued (${event})`);
    } else {
        console.log('Too many messages in the queue already..');
    }
}

for (let index = 0; index < 100; index++) {
    queueRequest(index);
}

app.get('/:id', (req, res) => {
    console.log(req.params);
    console.log(req.query);
    let object = { params: req.params, query: req.query }
    queueRequest(object);
    res.send('processing started')
})
