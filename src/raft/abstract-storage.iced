
class AbstractStorage
    constructor: (options) ->
        @logger = options?.logger or require "winston-color"

    set: (hash, callback) ->
        callback? { message: "NOT IMPLEMENTED" }, null

    get: (keys, callback) ->
        callback? { message: "NOT IMPLEMENTED" }, null

    appendEntries: (startIndex, entries, state, callback) ->
        callback? { message: "NOT IMPLEMENTED" }, null

    getEntries: (startIndex, callback) ->
        callback? { message: "NOT IMPLEMENTED" }, null

    load: (callback) ->
        await @get null, defer error, data
        return callback? error if error

        await @getEntries 0, defer error, data.entries
        callback? error, data


module.exports = AbstractStorage
