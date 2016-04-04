
{EventEmitter} = require "events"

AsyncWorkQueue = require "async-work-queue"

class Peer extends EventEmitter

    constructor: (@id, @metadata, @options, connection, @node) ->

        @transport = @options.transport
        @disconnected = false

        @outQueue = new AsyncWorkQueue (task, done) =>
            calledback = false
            callback = (args...) ->
                return if calledback
                calledback = true
                task.callback args...
                done()

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
        @connection.on "connected", (params...) => @emit "connected", params...
        @connection.on "connecting", => @emit "connecting"

        # @emit "connected"


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
        calledback = false
        done = (error, args) =>
            return if calledback
            calledback = true

            clearTimeout timeout if timeout
            if not error or not error.timeout
                @emit "response", error, args

            callback error, args

        timeout = null
        onTimeout = ->
            timeout = undefined
            error = new Error "invoke timeout after #{@node.options.commandTimeout} ms"
            error.timeout = true
            done error


        if @node.options and @node.options.commandTimeout
            timeout = setTimeout onTimeout, @node.options.commandTimeout

        @emit "outgoing call", type, parameters
        if not @connection
            callback new Error "not connected"
        else
            @connection.send type, parameters, done



    send: (type, parameters, callback) ->
    
        # TODO: AsyncWorkQueue - remove items whose type matches "type" parameter.
        #if type in ["AppendEntries", "RequestVote"]
        #   ...
        
        @outQueue.push { type, parameters, callback }



module.exports = Peer
