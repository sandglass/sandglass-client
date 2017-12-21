'use strict'

/**
 * This is a way to make private
 * properties in Javascript
 * documented on the mozilla
 * Javascript reference
 */
const _private = new WeakMap()
const internal = object => {
  if (!_private.has(object)) { _private.set(object, {}) }
  return _private.get(object)
}

module.exports = { internal }
