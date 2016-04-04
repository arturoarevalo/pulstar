extend = require "xtend"
cuid = require "cuid"

{EventEmitter} = require "events"

Log = require "./log"
Cluster = require "./cluster"
LogApplier = require "./log-applier"
Broadcast = require "./broadcast"
Peer = require "./peer"

Idle = require "./idle"
Standby = require "./standby"
Stopped = require "./stopped"
Follower = require "./follower"
Candidate = require "./candidate"
Leader = require "./leader"


class Node extends EventEmitter

    @DEFAULT_OPTIONS = 
        standby: false
        minElectionTimeout: 150
        maxElectionTimeout: 300
        heartbeatInterval: 50
        commandTimeout: 3000
        replicationStreamHighWaterMark: 10
        retainedLogEntries: 50
        metadata: {}


    constructor: (options) ->
        @setMaxListeners Infinity

        @options = extend Node.DEFAULT_OPTIONS, options

        @id = @options.id or cuid()
        @metadata = @options.metadata or {}

        @loaded = false
        @stopped = false

        @state =
            volatile:
                leaderId: null
                commitIndex: 0
                lastApplied: 0
            persisted:
                currentTerm: 0
                votedFor: null
                log: new Log @, @options
                peers: []

        @logApplier = new LogApplier @state.persisted.log, @, @options.storage
        @logApplier.on "error", (error) => @emit "error", error
        @logApplier.on "applied log", (logIndex) => @emit "applied log", logIndex
        @logApplier.on "done persisting", (lastApplied) => 
            @state.persisted.log.applied @state.volatile.lastApplied
            @emit "done persisting", lastApplied

        @on "election timeout", => @role.emit "election timeout"
        @on "joined", (peer) => @role.emit "joined", peer

        @role = null
        @setRole "idle"

        await @load defer error
        if error
            process.nextTick => @emit "error", error
        else
            @loaded = true
            @emit "loaded"

            if @options.standby
                @setRole "standby"
            else
                @setRole "follower"



    load: (callback) ->

        await @options.storage.loadMeta @id, defer error, results
        if not error and results
            @state.persisted = results
            @state.persisted.peers = @state.persisted.peers.map (desc) =>
                peer = new Peer desc.id, desc.metadata, @options, null, @
                @internalJoin peer
                return peer
            @state.persisted.log = new Log @, @options, @state.persisted.log

        await @options.storage.lastAppliedCommitIndex @id, defer error, results
        if not error and results
            @state.volatile.lastApplied = results


    save: (callback) ->
        state = extend @state.persisted,
            log: @state.persisted.log.streamline()
            peers: @state.persisted.peers.map (peer) => { id: peer.id, metadata: peer.metadata }

        @options.storage.saveMeta @id, state, callback


    onceLoaded: (callback) ->
        if @loaded
            callback()
        else
            @once "loaded", callback


    listen: (options, callback) ->
        if @server
            @server.close()

        listener = (id, metadata, connection) => @internalJoin id, metadata, connection

        @server = @options.transport.listen @id, options, listener, callback


    join: (id, metadata, callback) ->
        done = (error) =>
            if error and not callback
                @emit "error", error
            else if callback
                callback error

        if "function" is typeof metadata
            callback = metadata
            metadata = null

        return done "cannot join self" if id is @id

        await @ensureLeader defer error
        return done error if error

        cmd = ["add peer", id, metadata]
        await @command cmd, true, defer error
        done error


    leave: (id, callback) ->
        await @ensureLeader defer error
        return callback error if error

        cmd = ["remove peer", id]
        await @command cmd, true, defer error

        if not error and id is @id
            @setRole "stopped"
            fn = => @stop()
            setTimeout fn, 1000

        callback error


    peerMeta: (url) ->
        for peer in @state.persisted.peers
            return peer.metadata if peer.id is url

        return null


    internalJoin: (id, metadata, connection) ->
        return if id is @id

        # Check if the peer is in our list.
        index = -1
        for peer, i in @state.persisted.peers
            if peer.id is id
                peer.disconnect()
                peer.removeAllListeners()

                index = i
                break

        peer = new Peer id, metadata, @options, connection, @

        if index isnt -1
            @state.persisted.peers[index] = peer
        else
            @state.persister.peers.push peer

        if not connection
            peer.connect()

        onPeerCall = (type, args, callback) => @handlePeerCall peer, type, args, callback

        peer.on "call", onPeerCall
        peer.once "connection closed", => peer.removeListener "call", onPeerCall
        peer.on "outgoing call", (type, args) => @emit "outgoing call", type, args
        peer.on "response", (error, args) => @emit "response", peer, error, args
        peer.once "connected", => @emit "connected", peer
        peer.once "close", => @emit "close", peer
        peer.on "disconnected", => @emit "disconnected", peer
        peer.on "connecting", => @emit "connecting", peer

        if index isnt -1
            @emit "reconnected", peer
        else
            @emit "joined", peer


    internalLeave: (id) ->
        for peer, index in @state.persisted.peers
            if peer.id is id
                @state.persisted.peers.splice index, 1
                peer.disconnect()
                peer.removeAllListeners()
                @role.emit "left", peer
                @emit "left", peer

                break


    setRole: (role) ->
        @cancelElectionTimeout()
        @role.stop() if @role

        @role = switch role
            when "idle" then new Idle @, @options
            when "standby" then new Standby @, @options
            when "follower" then new Follower @, @options
            when "candidate" then new Candidate @, @options
            when "leader" then new Leader @, @options
            when "stopped" then new Stopped @, @options
            else throw new Error "unknown role #{role}"

        @role.on "error", (error) => @emit "error", error
        @role.on "warning", (warning) => @emit "warning", warning

        @emit "role", role, @
        @emit role, @


    broadcast: (type, request) -> new Broadcast @, @state.persisted.peers, type, request

    
    isMajority: (quorum) ->
        majority = Math.ceil (@state.persisted.peers.length + 1) / 2
        return quorum >= majority


    @property "currentTerm",
        get: -> @state.persisted.currentTerm
        set: (term) -> @state.persisted.currentTerm = term


    startElectionTimeout: ->
        @emit "reset election timeout"

        clearTimeout @electionTimeout if @electionTimeout

        fn = => @emit "election timeout"
        setTimeout fn, @randomElectionTimeout


    cancelElectionTimeout: ->
        clearTimeout @electionTimeout if @electionTimeout
        @electionTimeout = null


    @getter "randomElectionTimeout", -> @options.minElectionTimeout + Math.floor Math.random() * (@options.maxElectionTimeout - @options.minElectionTimeout)




    ###
    CLIENT API
    ###

    ensureLeader: (callback) ->
        error = null

        if "leader" isnt @role.name
            error = new Error "not the leader"
            error.code = "ENOTLEADER"
            error.leader = @state.volatile.leaderId

        callback error


    @getter "currentLeader", -> @state.volatile.leaderId


    command: (cmd, options, isInternal, callback) ->
        if "function" is typeof options
            callback = options
            options = undefined
            isInternal = undefined
        else if "function" is typeof callback
            callback = isInternal
            isInternal = undefined

        options = extend { timeout: @options.commandTimeout }, options

        await @ensureLoaded defer error
        return callback? error if error

        await @ensureLeader defer error
        return callback? error if error

        entry = 
            term: @currentTerm
            command: cmd

        if isInternal
            entry.topologyChange = true

        @state.persisted.log.push entry
        commitIndex = @state.persisted.log.length
        await @role.replicate commitIndex, options, defer error
        return callback? error if error

        @state.volatile.commitIndex = commitIndex
        await @logApplier.persist defer error
        return callback? error if error

        await @save defer error
        return callback? error if error

        options.waitForNodeLastApplied = commitIndex
        @role.replicate commitIndex, options, defer error

        callback? error


    handlePeerCall: (peer, type, request, callback) ->
        return if @stopped

        handleReplied = (args...) =>
            @emit "reply", args...

            if request.term and request.term > @currentTerm
                @currentTerm = request.term

            await @save defer error

            if error
                @emit "error", error
            else
                callback? null, args...

        @onceLoaded =>
            switch type
                when "AppendEntries"
                    @emit type, request
                    @role.onAppendEntries request, handleReplied
                when "RequestVote"
                    @emit type, request
                    @role.onRequestVote request, handleReplied
                when "InstallSnapshot"
                    @emit type, request
                    @role.onInstallSnapshot request, handleReplied
                else
                    @emit "error", new Error "unknown peer call type #{type}"


    applyTopologyChange: (entry) ->
        switch entry[0]
            when "add peer" then @internalJoin.call @, entry[1]
            when "remove peer" then @internalLeave.call @, entry[1]


    stop: (callback) ->
        if @stopped
            setImmediate callback if callback
            return

        @stopped = true
        @setRole "stopped"

        if @server
            @server.close callback
        else
            setImmediate callback

        for peer in @state.persisted.peers
            peer.disconnect()


module.exports = Node
