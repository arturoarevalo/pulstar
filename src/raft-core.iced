
require "coffee-script-properties"


class Log
    constructor: (@storage, @stateMachine, options) ->
        @logger = options?.logger or require "winston-color"

        # Persistent data.
        @entries = []
        @votedFor = 0
        @currentTerm = 0

        # Volatile data.
        @commitIndex = -1
        @lastApplied = -1


    @getter "lastTerm", -> @termAt @lastIndex

    @getter "lastIndex", -> @entries.length - 1

    termAt: (index) -> @entries[index]?.term or 0

    entryAt: (index) -> @entries[index]

    load: (callback) ->
        await @storage.load defer success, data

        if success
            @currentTerm = data.currentTerm or 0
            @votedFor = data.votedFor or 0
            @entries = data.entries or []





    appendEntries: (message, callback) ->
        # Check if caller is out of date.
        if message.term < @currentTerm
            return callback? false

        newEntries = message.entries or { startIndex: 0, values: [] }
        previousEntry = @entryAt message.prevLogIndex

        # Check if we're out of date.
        if (@lastIndex < message.prevLogIndex) or (previousEntry?.term isnt message.pervLogTerm)
            return callback? false

        # Check for heartbeats.
        if newEntries.length is 0
            @updateCommitIndex message.leaderCommit
            return callback? true

        # The currentTerm could only have changed if votedFor = 0 & votedFor can't
        # change in a term, so we can use it as a heuristic for when to write state.
        state = switch
            when @votedFor then {}
            else { currentTerm: @currentTerm, votedFor: 0 }

        await @storage.appendEntries newEntries.startIndex, newEntries.values, state, defer success

        if success
            @entries.splice newEntries.startIndex
            @entries = @entries.concat newEntries.values
            @updateCommitIndex message.leaderCommit

        callback? success


    
    requestVote: (message, callback) ->
        # Check if caller is out of date.
        if message.term < @currentTerm
            return callback? false

        if not @votedFor or @votedFor is message.candidateId
            if (message.lastLogTerm > @lastTerm) or (message.lastLogTerm is @lastTerm and message.lastLogIndex >= @lastIndex)

                @votedFor = message.candidateId
                await @storage.set { @votedFor, @currentTerm }, defer success

                return callback? success

        callback? false


