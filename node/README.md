## Sanglass Nodejs client


This is the Sanglass Nodejs client provides a convenient access to the Sandglass message queue via rpc calls.
It is written with async and the promise pattern so method calls must done with async in mind, which means you can use the `then` and `catch` or `async` and `await` which ones you prefer. Below is a basic usage of the client.


#### Creating a topic
```js
const client = new Client('7170')

const topic = 'futura'

client.createTopic({
  name: topic,
  replicationFactor: 1,
  numPartitions: 6,
})
  .then(res => console.log(res))
  .catch(err => console.log(`there was an error ${err}`))
  ```

#### Producing a message

Now
```js
client.produceMessage('emails', '', { dest: 'hi@example.com' })
  .then(res => console.log(res))
  .catch(err => console.log(`there was an error ${err}`))
```

#### Consuming
```js
client.listPartitions(topic)
  .then(partitions => client.newConsumer(
    topic,
    partitions[Object.keys(partitions)[0]],
    'group2',
    'consumer2'
  ))
  .then(consumer => consumer.consume())
  .then(stream => {
    stream.on('data', msg => {
      console.log(msg.value.toString())
    })
    stream.on('end', () => {
      console.log('done')
    })
    stream.on('error', err => {
      console.log('error', err)
    })
  })
  .catch(err => console.log('error', err))
```