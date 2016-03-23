Role = require "./role"

class CandidateRole extends Role

    constructor: (log, options) ->
        super log, "candidate", options
        @votes =
            self: true

    beginElection: =>
        @emit "changeRole", "candidate"

    assertRole: (message, rpc) ->
        if message.term > @log.currentTerm
            @log.currentTerm = message.term
            @log.votedFor = 0
            @clearElectionTimeout()
            @emit "changeRole", "follower"
        else if (message.term is @log.currentTerm) and (rpc is "appendEntries")
            @clearElectionTimeout()
            @emit "changeRole", "follower"

    countVote: (vote, totalPeers) ->
        if vote.voteGranted
            @votes[vote.id] = true

            votesCount = Object.keys(@votes).length
            majority = (totalPeers + 1) / 2
            @logger.info "[candidate]", "got votes from", @votes, "count =", votesCount, "majority =", majority
            if votesCount > majority
                @logger.info "[candidate]", "won election"
                @clearElectionTimeout()
                @emit "changeRole", "leader"

    request: (entry, callback) ->
        # Error ...
        error = 
            message: "not the leader"

        process.nextTick callback, error


module.exports = CandidateRole
