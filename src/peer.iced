
{EventEmitter} = require "events"

AsyncWorkQueue = require "async-work-queue"

class Peer extends EventEmitter

    constructor: (@id, @metadata, @options, connection, @node) ->

        @transport = @options.transport
        @disconnected = false

        @queues =
            out: new AsyncWorkQueue @invoke
            in: new AsyncWorkQueue @receive


        @outQueue = new AsyncWorkQueue (task, done) =>
            calledback = false
            callback = (args...) ->
                return if calledback
                calledback = true
                task.callback args...

            @invoke task.type, task.parameters, callback

        @inQueue = new AsyncWorkQueue (task, done) =>
            callback = (args...) ->
                task.callback args...
                done()

            @emit "call", task.type, task.parameters, callback

        if connection
            @setupConnection connection


    setupConnection: (connection) ->
        @connection = connection

        @connection.receive (type, parameters, callback) =>
            @inQueue.push { type, parameters, callback }

        @connection.once "close", => @emit "close"

        @connection.on "error", (error) => 
            @emit "error", error unless @disconnected

        @connection.on "disconnected", => @emit "disconnected"
        @connection.on "connected", => @emit "connected"
        @connection.on "connecting", => @emit "connecting"

        @emit "connected"


    connect: ->
        connection = @transport.connect @node.id, @node.metadata, @id, @metadata
        @setupConnection connection
        return connection


    disconnect: ->
        @disconnected = true
        @connection.close (error) =>
            if error
                @emit "error", error
            else
                @emit "connection closed"


    invoke: (type, parameters, callback) ->

        done = (error, args) =>


module.exports = Peer
