{EventEmitter} = require "events"

class DummyConnection extends EventEmitter

    constructor: (@id, @hub) ->


    delay: (timeout, fn) ->
        setTimeout fn, timeout

    
    send: (type, parameters, callback) ->
        await @delay 5, defer()

        @emit "send", @id, type, parameters

        fn = @hub.out[@id]
        if fn
            fn.call null, type, parameters, callback
        else
            callback.call null, new Error "cannot connect to #{@id}"


    receive: (callback) ->
        @hub.in[@id] = callback


    close: (callback) ->
        if @hub.out[@id]
            delete @hub.out[@id]

        if @hub.in[@id]
            delete @hub.in[@id]

        await @delay 5, defer()

        @emit "close"
        callback?()


module.exports = DummyConnection
