const Client = require('../client')

const client = new Client('7170')

const topic = 'futura'
const msg = Buffer.from('Hello, Sandglass!', 'hex')

client.produceMessage(topic, '', { value: msg })
  .then(res => {
    let offsets = res.offsets
    let value = offsets[0].toString('hex')
    console.log(value)
  })
  .catch(err => console.log('err'))
