require "coffee-script-properties"

{Readable, Writable} = require "stream"

class DummyStorage

    constructor: ->

        @store =
            meta: {}
            state: {}
            commands: {}


    @getter "random", -> Math.floor Math.random() * 5

    delay: (callback) ->
        setTimeout callback @random


    saveMeta: (id, state, callback) ->
        await @delay defer()

        @store.meta[id] = JSON.stringify state
        callback()


    loadMeta: (id, callback) ->
        await @delay defer()

        data = @store.meta[id]
        data = JSON.parse data if data

        setImmediate callback, null, data


    applyCommand: (id, commitIndex, command, callback) ->
        await @delay defer()

        @store.commands[id] ?= []
        @store.commands[id].push command
        @store.state[id] = commitIndex

        callback()


    lastAppliedCommitIndex: (id, callback) ->
        await @delay defer()

        setImmediate callback, null, @store.state[id]


    saveCommitIndex: (id, commitIndex, callback) ->
        await @delay defer()

        @store.state[id] = commitIndex
        callback()


    createReadStream: (id) ->
        commandIndex = -1
        finished = false
        commands = @store.commands[id] or []
        length = commands.length

        stream = new Readable { objectMode: true }
        stream._read = ->
            commandIndex++
            command = null
            if commandIndex < length
                command = commands and commands[commandIndex]

            if not command and not finished
                finished = true
                stream.push null
            else if command
                stream.push command

        return stream


    createWriteStream: (id) ->
        commands = @store.commands[id]

        stream = new Writable { objectMode: true }
        stream._write = (chunk, encoding, callback) ->
            commands.push chunk
            setImmediate callback

        return stream


    removeAllState: (id, callback) ->
        await @delay defer()

        @store.commands[id] = []
        callback()


module.exports = DummyStorage
