Role = require "./role"

class FollowerRole extends Role

    constructor: (log, options) ->
        super log, "follower", options

        @leaderId = 0

    beginElection: =>
        @emit "changeRole", "candidate"

    assertRole: (message) ->
        if message.term > @log.currentTerm
            @log.currentTerm = message.term
            @log.votedFor = 0

    requestVote: (vote, callback) ->
        await @log.requestVote vote, defer error, voteGranted

        if not error and voteGranted
            @resetElectionTimeout()

        callback? error, voteGranted

    appendEntries: (message, callback) ->
        @leaderId = message.leaderId
        @resetElectionTimeout()
        @log.appendEntries message, callback

    request: (entry, callback) ->
        # Error ...
        error = 
            message: "not the leader"
            leaderId: @leaderId

        process.nextTick callback, error


module.exports = FollowerRole
