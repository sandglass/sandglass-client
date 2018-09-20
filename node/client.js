'use strict'

const sgproto = require('@sandglass/grpc')
const grpc = require('grpc')

const Consumer = require('./consumer')

/**
 * @typedef {Object} Client
 *
 * Sandglass client
 */
module.exports = class Client {

  /**
   * Sandglass client
   * @param {String} address
   */
  constructor(address) {
    this.client = new sgproto.BrokerService(`:${address}`, grpc.credentials.createInsecure())
  }

  /**
   * Create a Topic
   *
   * @param {Object} params
   * @returns {Object} TopicReply
   */
  async createTopic(params) {
    return new Promise((resolve, reject) => {

      this.client.CreateTopic(params, (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   * Get a Topic
   *
   * @param {String} topic
   */
  async listPartitions(topic) {
    return new Promise((resolve, reject) => {

      this.client.GetTopic({ name: topic }, (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   * Produce a message
   *
   * @param {String} topic
   * @param {String} partition
   * @param {Object} msg
   */
  async produceMessage(topic, partition, msg) {
    return new Promise((resolve, reject) => {

      this.client.Produce({
        topic: topic,
        partition: partition,
        messages: msg,
      },
      (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   * Returns a stream of messages
   *
   * @param {String} topic
   * @param {String} partition
   */
  async produceMessageStream(topic, partition) {
    return new Promise((resolve, reject) => {

      const meta = new grpc.Metadata()
      meta.add('topic', topic)
      meta.add('partition', partition)

      this.client.ProduceMessagesStream(meta, (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   * Returns a consumer instance
   *
   * @param {String} topic
   * @param {String} partition
   * @param {String} group
   * @param {String} name
   */
  async newConsumer(topic, partition, group, name) {
    return new Consumer(this.client, topic, partition, group, name)
  }

  /**
   * Close client
   */
  async close() {
    await grpc.closeClient(this)
  }

}
