
Log = require "./log"
FollowerRole = require "./follower-role"
CandidateRole = require "./candidate-role"
LeaderRole = require "./leader-role"


class RaftServer 

    constructor: (@id, @storage, @stateMachine, @options) ->
        @role = null
        @log = new Log @storage, @stateMachine, @options
        @requests = {}
        @peers = []
        @peerMap = {}

        @log.on "execute", @onExecuted


    onChangeRole: (name) ->
        if @role
            @role.removeListener "changeRole", @onChangeRole
            @role.removeListener "appendEntries", @nAppendEntries

        switch name
            when "follower" 
                @role = new FollowerRole @log, @options
                @role.resetElectionTimeout()

            when "candidate"
                @role = new CandidateRole @log, @options
                @role.resetElectionTimeout()
                @role.beginElection()

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

    
    onAppendEntries: (id, message) ->
        # TODO : CHANNEL COMUNNICATION
         
        message.leaderId = @id
        peer = @peerMap[id]
        return if not peer

        await peer.appendEntries message, defer error, response

        # TODO: error checking
        @role.assertRole response
        @role.entriesAppended id, message, response


    start: (peers, role) ->
        role ?= "follower"
        @peers = peers
        for peer in @peers
            @peerMap[peer.id] = peer

        await @log.load defer success
        @onChangeRole role


    beginElection: ->
        @log.currentTerm++
        message =
            term: @log.currentTerm
            lastLogIndex: @log.lastIndex
            lastLogTerm: @log.lastTerm

        await @log.requestVote message, defer success

        for peer in @peers
            await peer.requestVote message, defer vote
            @countVote vote

    countVote: (vote) ->
        return if vote.term < @log.currentTerm

        @role.assertRole vote
        @role.countVote vote, @peers.length


    requestVote: (message, callback) ->
        @role.assertRole message
        await role.requestVote message, defer voteGranted
        callback? @requestVoteResponse voteGranted

    requestVoteResponse: (voteGranted) ->
        return {
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
