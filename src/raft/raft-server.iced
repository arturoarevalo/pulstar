
Log = require "./log"
FollowerRole = require "./follower-role"
CandidateRole = require "./candidate-role"
LeaderRole = require "./leader-role"

{EventEmitter} = require "events"


class RaftServer extends EventEmitter

    constructor: (@id, @storage, @stateMachine, @options) ->
        @logger = options?.logger or require "winston-color"
        @role = null
        @log = new Log @storage, @stateMachine, @options
        @requests = {}
        @peers = []
        @peerMap = {}

        @log.on "execute", @onExecuted


    onChangeRole: (name) =>
        if @role
            @role.removeListener "changeRole", @onChangeRole
            @role.removeListener "appendEntries", @onAppendEntries
            @logger.info @id, "new role", name
        else
            @logger.info @id, "initial role", name

        switch name
            when "follower" 
                @role = new FollowerRole @log, @options
                @role.resetElectionTimeout()

            when "candidate"
                @role = new CandidateRole @log, @options
                @role.resetElectionTimeout()
                @beginElection()

            when "leader"
                peers = Object.keys @peerMap
                @role = new LeaderRole @log, peers, @options
                @role.noop()

        @role.on "changeRole", @onChangeRole
        @role.on "appendEntries", @onAppendEntries


    onExecuted: (index, entry, result) ->
        request = @requests[index]
        delete @requests[index]
        
        request?.callback? result

    
    onAppendEntries: (id, message) =>
        return if id is @id

        # TODO : CHANNEL COMUNNICATION
        message.leaderId = @id
        peer = @peerMap[id]
        return if not peer

        @logger.info @id, "calling appendEntries on", id, message
        await peer.appendEntries message, defer error, response
        @logger.info @id, "calling appendEntries on", id, "response", error, response

        # TODO: error checking
        @role.assertRole response
        @role.entriesAppended id, message, response


    start: (peers, role) ->
        role ?= "follower"
        @peers = peers
        for peer in @peers #when peer.id isnt @id
            @peerMap[peer.id] = peer

        await @log.load defer success

        @onChangeRole role


    beginElection: ->
        @log.currentTerm++
        message =
            term: @log.currentTerm
            lastLogIndex: @log.lastIndex
            lastLogTerm: @log.lastTerm

        @logger.info @id, "log request vote message", message

        await @log.requestVote message, defer error, success

        @logger.info @id, "log request vote success", error, success

        for peer in @peers when peer.id isnt @id
            await peer.requestVote message, defer error, vote
            @logger.info "peer request vote response", vote
            @countVote vote

    countVote: (vote) ->
        return if vote.term < @log.currentTerm

        @role.assertRole vote
        @role.countVote vote, @peers.length


    requestVote: (message, callback) ->
        @logger.info @id, "peer request vote", message
        @role.assertRole message
        await @role.requestVote message, defer error, voteGranted

        callback? error, {
            id: @id
            term: @log.currentTerm
            voteGranted: voteGranted
        }


    appendEntries: (message, callback) ->
        @role.assertRole message
        await @role.appendEntries message, defer error, success
        callback? error, 
            term: @log.currentTerm
            success: success

    
    request: (entry, callback) ->
        message = 
            term: @log.currentTerm
            leaderId: @id
            prevLogIndex: @log.lastIndex
            prevLogTerm: @log.lastTerm
            leaderCommit: @log.commitIndex
            entries: 
                startIndex: @log.lastIndex + 1
                values: [{ term: @log.currentTerm, op: entry }]

        await @role.request message, defer error

        if error
            callback? error
        else
            index = message.entries.startIndex
            @requests[index] = { message, callback }


module.exports = RaftServer
