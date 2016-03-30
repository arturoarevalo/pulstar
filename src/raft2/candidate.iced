Role = require "role"

class Candidate extends Role

    constructor: (node, @options) ->
        super node, "candidate"

        @node.state.persisted.votedFor = @node.id
        @votedForMe = 0

        @node.startElectionTimeout()
        @once "election timeout", @onElectionTimeout

        @node.state.persisted.currentTerm++

        @countVote()

        lastLog = null
        if not @stopped
            if @node.state.persisted.log.length
                lastLog = @node.state.persisted.log.entryAt @node.state.persisted.log.length

            request =
                term: @node.satte.persisted.currentTerm
                candidateId: @node.id
                lastLogIndex: @node.state.persisted.log.length
                lastLogTerm: lastLog and lastLog.term

            broadcast = @node.broadcast "RequestVote", request
            broadcast.on "response", @onBroadcastResponse


    countVote: ->
        @votedForMe++
        if @node.isMajority @votedForMe
            @broadcast?.cancel()
            setImmediate => 
                @node.role = "leader"


    onElectionTimeout: =>
        @node.role = "candidate"

    onBroadcastResponse: (error, response) =>
        if not @stopped and response?.voteGranted
            @countVote()


module.exports = Candidate
