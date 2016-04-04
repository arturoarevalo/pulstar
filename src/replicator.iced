{EventEmitter} = require "events"

class Replicator extends EventEmitter

    constructor: (@node, @peers, @options) ->
        @setMaxListeners Infinity
        @replicate()


    replicate: ->
        @scheduleHeartbeat()
        @replicateToPeer id for id in Object.keys @peers


    scheduleHeartbeat: ->
        if @interval
            clearInterval @interval

        @interval = setInterval @heartbeat, @options.heartbeatInterval

    heartbeat: =>
        @emit "heartbeat"
        @replicate()

    stop: ->
        @removeAllListeners()
        clearInterval @interval


    replicateToPeer: (id) ->
        peer = @peers[id]
        index = peer.nextIndex
        log = @node.state.persisted.log
        entries = null

        if peer.nextIndex <= log.lastIncludedIndex and @node.state.volatile.commitIndex > 0
            @streamSnapshotToPeer id
        else
            if log.length >= index
                entries = [log.entryAt index]
            else
                entries = []

            request = 
                term: @node.currentTerm
                leaderId: @node.id
                prevLogIndex: index - 1
                entries: entries
                leaderCommit: @node.state.volatile.commitIndex

           await peer.meta.send "AppendEntries", request, defer args...
           @emit ["response", peer.meta.id, index, entries.length, args...]


    retry: (id) ->
        @replicateToPeer id


    streamSnapshotToPeer: (id) ->
        peer = @peers[id]
        nextIndex = @node.state.volatile.commitIndex + 1

        if peer and not peer.streaming
            peer.streaming = true

            rs = @node.options.storage.createReadStream @node.id
            ws = peer.meta.createWriteStream
            rs.pipe(ws).once "finish", =>
                peer.streaming = false
                peer.nextIndex = nextIndex
                @replicate()


module.exports = Replicator
