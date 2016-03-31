
{EventEmitter} = require "events"

AsyncWorkQueue = require "async-work-queue"

class Peer extends EventEmitter

    constructor: (@id, @metadata, @options, connection, @node) ->

        @transport = @options.transport
        @disconnected = false

        @queues =
            out: new AsyncWorkQueue @invoke
            in: new AsyncWorkQueue @receive

        if connection
            @setupConnection connection



    invoke: (task, done) =>

        calledback = false
        callback = (args...) ->
            return if calledback
            calledback = true
            task.callback args...

        @internalInvoke task.type, task.args, callback


    receive: (task, cb) =>
        @emit "call", task.type, task.args, (args...) =>
            task.callback args...
            cb()




module.exports = Peer
