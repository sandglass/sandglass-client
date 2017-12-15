'use strict'

const { internal } = require('./util')

/**
 * @typedef {Object} Consumer
 *
 * Sandglass consumer
 */
module.exports = class Consumer {

  /**
   * Sandglass consumer
   *
   * @param {Object} client
   * @param {String} topic
   * @param {String} partition
   * @param {String} group
   * @param {String} name
   */
  constructor(client, topic, partition, group, name) {
    internal(this).client = client
    internal(this).topic = topic
    internal(this).partition = partition
    internal(this).group = group
    internal(this).name = name
  }

  async consume() {
    return new Promise((resolve, reject) => {

      internal(this).client.ConsumeFromGroup({
        topic: internal(this).topic,
        partition: internal(this).partition,
        consumerGroupName: internal(this).group,
        consumerName: internal(this).name,
      },
      (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {Object} msg
   */
  async acknowledge(msg) {
    return new Promise((resolve, reject) => {

      if (typeof msg.offset === 'undefined') return new Error(`offset must be defined`)
      if (msg.offset.length === 0) return new Error(`offset should not be emty`)
      if (Array.isArray(msg.offset) === false) return new Error(`offset must be an array`)

      internal(this).client.Acknowledge({
        topic: internal(this).topic,
        partition: internal(this).partition,
        consumerGroupName: internal(this).group,
        consumerName: internal(this).name,
        offset: msg.offset,
      },
      (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {Object} msg
   */
  async notAcknowledge(msg) {
    return new Promise((resolve, reject) => {

      if (typeof msg.offset === 'undefined') return new Error(`offset must be defined`)
      if (msg.offset.length === 0) return new Error(`offset should not be emty`)
      if (Array.isArray(msg.offset) === false) return new Error(`offset must be an array`)

      internal(this).client.NotAcknowledge({
        topic: internal(this).topic,
        partition: internal(this).partition,
        consumerGroupName: internal(this).group,
        consumerName: internal(this).name,
        offset: msg.offset,
      },
      (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {Array} offsets
   */
  async acknowledgeMessages(offsets) {
    return new Promise((resolve, reject) => {

      if (typeof offsets === 'undefined') return new Error(`offsets must be defined`)
      if (offsets.length === 0) return new Error(`offsets should not be emty`)
      if (Array.isArray(offsets) === false) return new Error(`offsets must be an array`)

      internal(this).client.AcknowledgeMessages({
        topic: internal(this).topic,
        partition: internal(this).partition,
        consumerGroupName: internal(this).group,
        consumerName: internal(this).name,
        offset: offsets,
      },
      (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

  /**
   *
   * @param {Object} msg
   */
  async commit(msg) {
    return new Promise((resolve, reject) => {

      if (typeof msg.offset === 'undefined') return new Error(`offset must be defined`)
      if (msg.offset.length === 0) return new Error(`offset should not be emty`)
      if (Array.isArray(msg.offset) === false) return new Error(`offset must be an array`)

      internal(this).client.Commit({
        topic: internal(this).topic,
        partition: internal(this).partition,
        consumerGroupName: internal(this).group,
        consumerName: internal(this).name,
        offset: msg.offset,
      },
      (err, resp) => {
        if (err) return reject(err)
        return resolve(resp)
      })
    })
  }

}
