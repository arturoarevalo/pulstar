Role = require "./role"
Replicator = require "./replicator"

class Leader extends Role

    constructor: (node, @options) ->
        super node, "leader"

        @interval = undefined
        @peers = {}

        @addPeer peer for peer in @node.state.persisted.peers
        @on "joined", @addPeer
        @on "reconnected", @addPeer
        @on "left", @removePeer

        @node.state.volatile.leaderId = @node.id

        @replicator = new Replicator node, @peers, @options
        @replicator.on "error", (error) => @emit "error", error
        @replicator.on "response", @onReplicateResponse

        @once "stopped", =>
            @removeListener "joined", @addPeer
            @removeListener "reconnected", @addPeer
            @removeListener "left", @removePeer
            @replicator.removeListener "response", @onReplicateResponse
            @replicator.stop()


    addPeer: (peer) =>
        @peers[peer.id] =
            meta: peer
            nextIndex: @node.state.persisted.log.length + 1

    removePeer: (peer) =>
        delete @peers[peer.id]


    onReplicateResponse: (id, logIndex, entryCount, error, request) =>
        peer = @peers[id]
        return if not peer

        switch
            when error
                @emit "warning", error

            when request and request.term > @node.currentTerm
                @node.currentTerm = request.term
                @node.role = "follower"

            when request and request.success
                peer.nextIndex = logIndex + entryCount
                setImmediate => @emit "replication success", id, logIndex, request.lastApplied

            else
                if "number" is typeof request.lastApplied
                    peer.nextIndex = Math.max request.lastApplied + 1
                else
                    peer.nextIndex = Math.max peer.nextIndex - 1, 0

                @replicator.retry id


    replicate: (logIndex, options, callback) ->
        if "function" is typeof options
            cb = options
            options = {}

        options ?= {}

        yep = 1
        done = {}
        lastApplieds = {}
        timeout = null
        replied = false

        if not maybeStop()
            if options.timeout > 0
                timeout = setTimeout timedout, options.timeout
                timeout.unref()

            @on "replication success", onReplicationSuccess
            @replicator.replicate()


        onReplicationSuccess = (id, peerLogIndex, lastApplied) =>
            if not done[id] and peerLogIndex >= logIndex
                done[id] = true
                yep++
            lastApplieds[id] = lastApplied
            maybeStop()


        maybeStop = () =>
            stop = shouldStop()
            if stop
                reply()
            return stop

        shouldStop = () =>
            stop = false

            if @node.isMajority yep
                if not options.waitForNode or options.waitForNode is @node.id
                    stop = true
                else if done[options.waitForNode]
                    if options.waitForNodeLastApplied
                        if lastApplieds[options.waitForNode] >= options.waitForNodeLastApplied
                            stop = true
                    else
                        stop = true

            return stop


        timedout = () =>
            timeout = undefined
            reply new Error "timedout after #{options.timeout} ms trying to replicate log index #{logIndex}"


        reply = (error) =>
            if not replied
                replied = true

                @removeLister "replication success", onReplicationSuccess
                if timeout
                    clearTimeout timeout
                    timeout = undefined

                callback? error


module.exports = Leader
