module.exports = Broadcast

{EventEmitter} = require "events"

class Broadcast extends EventEmitter

    constructor: (@node, @peers, @type, @args) ->
        @node.onceLoaded => @broadcast peer for peer in @peers

    broadcast: (peer) ->
        await peer.send @type, @args, defer args...

        args.unshift "response"
        @emit args...
        return 

    cancel: ->
        @removeAllListeners()
