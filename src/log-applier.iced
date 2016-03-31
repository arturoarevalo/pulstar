{EventEmitter} = require "events"

class LogApplier extends EventEmitter

    constructor: (@log, @node, @storage) ->
        @setMaxListeners Infinity
        @persisting = false

        @persist()


    persist: (callback) ->
        state = @node.state.volatile
        toApply = state.lastApplied - 1

        done = (error) =>
            @persisting = false
            @emit "error", error if error and not callback
            callback? error

        persisted = (error) =>
            return done error if error

            @persisting = false
            state.lastApplied = toApply
            @emit "applied log", toApply
            @persist callback

        if @persisting
            @once "done persisting", callback if callback
            return

        if state.commitIndex <= state.lastApplied
            @emit "done persisting", state.lastApplied
            return done()

        @persisting = true
        entry = @node.state.persisted.log.entryAt toApply
        return done() if not entry

        if entry.topologyChange
            @storage.saveCommitIndex @node.id, toApply, (error) =>
                if error
                    done error
                else
                    @node.save persisted
        else
            @storage.applyCommand @node.id, toApply, entry.command, persisted

module.exports = LogApplier
