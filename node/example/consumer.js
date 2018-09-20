const { execSync } = require('child_process')

const Client = require('../client')

const client = new Client('7170')

const topic = 'futura'

client.createTopic({
  name: topic,
  replicationFactor: 1,
  numPartitions: 6,
})
  .then(res => console.log(res))
  .catch(err => console.log(`there was an error ${err}`))

// execute shell command for producing messages
execSync(`sandctl produce futura '{"dest" : "hi@example.com"}' -n 10`)

async function consume(topic) {

  try {
    const partitions = await client.listPartitions(topic)
    const partition = partitions.partitions[0]
    const consumer = await client.newConsumer(topic, partition, 'group1', 'consumer1')
    const stream = await consumer.consume()

    return stream

  } catch (error) {
    console.log(`there was an error ${error}`)
  }
}

consume(topic)
  .then(stream => {
    stream.on('data', msg => {
      console.log(msg)
    })
    stream.on('end', () => {
      console.log('done')
    })
    stream.on('error', err => {
      console.log('error', err)
    })
  })
  .catch(err => console.log(`there was an error ${err}`))

client.close()
