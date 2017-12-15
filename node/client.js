'use strict'

const sgproto = require('@sandglass/grpc')
const grpc = require('grpc')

const Consumer = require('./consumer')
const { internal } = require('./util')

/**
 * @typedef {Object} Client
 *
 * Sandglass client
 */
module.exports = class Client {

  /**
   * Sandglass client
   */
  constructor() {
    internal(this).client = new sgproto.BrokerService(':7170', grpc.credentials.createInsecure())
  }

  /**
   *
   * @param {Object} params
   */
  async createTopic(params) {
    return new Promise((resolve, reject) => {

      internal(this).client.CreateTopic(params, (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {String} topic
   */
  async listPartitions(topic) {
    return new Promise((resolve, reject) => {

      internal(this).client.GetTopic({ name: topic }, (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {String} topic
   * @param {String} partition
   * @param {Object} msg
   */
  async produceMessage(topic, partition, msg) {
    return new Promise((resolve, reject) => {

      internal(this).client.Produce({
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
   *
   * @param {String} topic
   * @param {String} partition
   */
  async produceMessageStream(topic, partition) {
    return new Promise((resolve, reject) => {

      const meta = new grpc.Metadata()
      meta.add('topic', topic)
      meta.add('partition', partition)

      internal(this).client.ProduceMessagesStream(meta, (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {String} topic
   * @param {String} partition
   * @param {String} group
   * @param {String} name
   */
  newConsumer(topic, partition, group, name) {
    return new Consumer(internal(this).client, topic, partition, group, name)
  }

  close() {
    return grpc.closeClient(this)
  }

}
