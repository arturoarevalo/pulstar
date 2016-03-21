Role = require "./role"

class LeaderRole extends Role

    constructor: (log, peers, options) ->
        super log, "leader", options

        @peers = peers
        @nextIndex = {}
        @matchIndex = {}
        @hearbeatInterval = options?.hearbeatInterval or 50
        @heartbeatTimer = null


    clearHeartbeat: ->
        if @heartbeatTimer
            clearTimeout @heartbeatTimer
            @heartbeatTimer = null

    broadcastEntries: ->
        @clearHeartbeat
        @sendAppendEntries id for id in @peers
        @heartbeatTimer = setTimeout @broadcastEntries, @hearbeatInterval


    sendAppendEntries: (id) ->
        prevLogIndex = (@nextIndex[id] or @log.lastIndex) - 1
        message =
            term: @log.currentTerm
            prevLogIndex: prevLogIndex
            prevLogTerm: @log.termAt prevLogIndex
            leaderCommit: @log.commitIndex
            entries: @log.entriesSince prevLogIndex

        @emit "appendEntries", id, message


    noop: ->
        @request
            term: @log.currentTerm
            prevLogIndex: @log.lastIndex
            prevLogTerm: @log.lastTerm
            leaderCommit: @log.commitIndex
            entries:
                startIndex: @log.lastIndex + 1
                values: [{ term: @log.currentTerm, noop: true, op: {}}]


    assertRole: (message) ->
        if message.term > @log.currentTerm
            @clearHeartbeat()
            @log.currentTerm = message.term
            @log.votedFor = 0
            @emit "changeRole", "follower"


    updateCommitIndex: ->
        lastIndex = @log.lastIndex
        return if @log.commitIndex is lastIndex

        majority = Math.floor @peers.length / 2
        matchIndices = [lastIndex]
        for id in @peers
            matchIndices.push @matchIndex[id] or -1

        matchIndices.sort()
        majorityIndex = matchIndices[majority]

        if @log.termAt(majorityIndex) is @log.currentTerm
            @log.updateCommitIndex majorityIndex


    entriesAppended: (id, message, response) ->
        if response.success
            matchIndex = message.entries.startIndex + message.entries.values.length - 1
            @matchIndex[id] = matchIndex
            @nextIndex[id] = matchIndex + 1
            if matchIndex > @log.commitIndex
                @updateCommitIndex()
        else
            # If AppendEntries fails because of log inconsistency, decrement nextIndex
            # and retry (§5.3)
            @nextIndex[id] = message.prevLogIndex
            @sendAppendEntries id


    request: (message, callback) ->
        await @log.appendEntries message, defer error, success

        if not error
            @broadcastEntries()

        callback? error, not error


module.exports = LeaderRole
