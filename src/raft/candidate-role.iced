Role = require "./role"

class CandidateRole extends Role

    constructor: (log, options) ->
        super log, "candidate", options
        @votes =
            self: true

    beginElection: ->
        @emit "changeRole", "candidate"

    assertRole: (message, rpc) ->
        if message.term > @log.currentTerm
            @log.currentTerm = message.currentTerm
            @log.votedFor = 0
            @clearElectionTimeout()
            @emit "changeRole", "follower"
        else if (message.term is @log.currentTerm) and (rpc is "appendEntries")
            @clearElectionTimeout()
            @emit "changeRole", "follower"

    countVote: (vote, totalPeers) ->
        if vote.voteGranted
            @votes[vote.id] = true
            if Object.keys(@votes).length > (totalPeers + 1) / 2
                @clearElectionTimeout()
                @emit "changeRole", "leader"

    request: (entry, callback) ->
        # Error ...
        callback? { message: "not the leader" }


module.exports = CandidateRole
