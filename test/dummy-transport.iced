DummyConnection = require "./dummy-connection"

{EventEmitter} = require "events"

class DummyTransport extends EventEmitter
    
    @HUB =
        in: {}
        out: {}


    connect: (nodeId, nodeMetadata, id, metadata, options) ->
        @emit "connect", nodeId, nodeMetadata, id, metadata, options
        return new DummyConnection id, DummyTransport.HUB


    listen: (localId, id, callback) ->
        DummyTransport.HUB.out[id] = callback


    invoke: (id, parameters...) ->
        await process.nextTick defer()

        fn = DummyTransport.HUB.in[id]
        if fn
            parameters.shift()
            fn null, parameters...


module.exports = DummyTransport
