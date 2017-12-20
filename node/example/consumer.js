const Client = require('../client')

const client = new Client('7170')

const topic = 'futura'

// client.createTopic({
//   name: topic,
//   replicationFactor: 1,
//   numPartitions: 6,
// })
//   .then(res => console.log(res))
//   .catch(err => console.log(`there was an error ${err}`))

async function consume(topic) {

  try {

    const partitions = await client.listPartitions(topic)

    const consumer = await client.newConsumer(
      topic,
      partitions[Object.keys(partitions)[0]],
      'group2',
      'consumer2'
    )

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

// const start = process.hrtime()

// const end = process.hrtime(start)
