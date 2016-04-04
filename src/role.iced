{EventEmitter} = require "events"

class Role extends EventEmitter

    constructor: (@node, @name) ->
        @setMaxListeners Infinity
        @stopped = false
        @installingSnapshot = false

        @once "stopped", =>
            @on "error", ->
                # don't care about errors after stopped

    stop: ->
        @stopped = true
        setImmediate => @emit "stopped"
        

    onAppendEntries: (request, callback) ->
        if request.term >= @node.currentTerm
            @node.currentTerm = request.term
            @node.state.volatile.leaderId = request.leaderId
            @node.role = "follower"
            @node.onAppendEntries request, callback
        else
            callback null,
                term: @node.currentTerm
                success: false
                reason: "term is behind current term"


    onRequestVote: (request, callback) ->
        currentTerm = @node.currentTerm
        state = @node.state.persisted

        granted = (request.term >= currentTerm) and
            (not state.votedFor or state.votedFor is request.candidateId) and
            ((state.log.last and state.log.last.term < request.lastLogTerm) or 
                (request.lastLogIndex >= state.log.length))

        if granted
            state.votedFor = request.candidateId
            @emit "vote granted", state.votedFor

        callback null,
            term: @node.currentTerm
            voteGranted: granted


    onInstallSnapshot: (request, callback) ->
        if request.term >= @node.currentTerm
            @node.currentTerm = request.term
            @node.state.volatile.leaderId = request.leaderId
            @node.role = "follower"
            @node.onInstallSnapshot request, callback
        else
            callback null,
                term: @node.currentTerm
                success: false
                reason: "term is behind current term"


module.exports = Role
