AbstractStorage = require "./abstract-storage"

class MemoryStorage extends AbstractStorage

    constructor: (options) ->
        super options

        @state = {}
        @entries = []

    set: (hash, callback) ->
        for key, value of hash
            @state[key] = value

        callback? null
 
    get: (keys, callback) ->
        keys ?= Object.keys @state

        result = {}
        for key in keys
            result[key] = state[key]

        callback? null, result
   
    appendEntries: (startIndex, entries, state, callback) ->
        state ?= {}

        if @entries.length isnt startIndex
            @entries.splice startIndex
        @entries = @entries.concat entries

        @set state, callback
   
    getEntries: (startIndex, callback) ->
        callback? null, @entries.slice startIndex


module.exports = MemoryStorage
