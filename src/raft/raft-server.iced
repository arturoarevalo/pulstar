
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
            @logger.info "new role ->", name
        else
            @logger.info "initial role ->", name

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

        @logger.debug "RPC to [#{id}]", "appendEntries", "request", message
        @callRPC id, "appendEntries", message


    start: (peers, role) ->
        role ?= "follower"
        @peers = peers
        for peer in @peers #when peer.id isnt @id
            @peerMap[peer.id] = peer

        await @log.load defer success

        @onChangeRole role


    callRPC: (id, method, message) ->
        peer = @peerMap[id]
        return if not peer

        process.nextTick peer[method], message


    beginElection: ->
        @log.currentTerm++
        message =
            term: @log.currentTerm
            candidateId: @id
            lastLogIndex: @log.lastIndex
            lastLogTerm: @log.lastTerm

        @logger.info "begin election"

        await @log.requestVote message, defer error, success

        for peer in @peers when peer.id isnt @id
            @callRPC peer.id, "requestVote", message

    requestVote: (message) =>
        @logger.debug "RPC from [#{message.candidateId}]", "requestVote", "request =", message

        @role.assertRole message
        await @role.requestVote message, defer error, voteGranted

        response = 
            id: @id
            term: @log.currentTerm
            voteGranted: voteGranted

        @logger.debug "RPC from [#{message.candidateId}]", "requestVote", "error =", error, "response =", response

        @callRPC message.candidateId, "requestVoteResponse", response


    requestVoteResponse: (message) =>
        return if message.term < @log.currentTerm

        @role.assertRole message
        @role.countVote message, @peers.length


    appendEntries: (message) =>
        @role.assertRole message, "appendEntries"
        await @role.appendEntries message, defer error, success

        response =
            term: @log.currentTerm
            id: @id
            matchIndex: message.entries.startIndex + message.entries.values.length - 1
            prevLogIndex: message.prevLogIndex
            success: success

        @callRPC message.leaderId, "appendEntriesResponse", response


    appendEntriesResponse: (message) =>
        @role.assertRole message
        @role.entriesAppended message.id, message


    
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
