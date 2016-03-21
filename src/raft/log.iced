require "coffee-script-properties"
{EventEmitter} = require "events"

class Log extends EventEmitter
    constructor: (@storage, @stateMachine, options) ->
        super

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
        await @storage.load defer error, data

        if not error and data
            @currentTerm = data.currentTerm or 0
            @votedFor = data.votedFor or 0
            @entries = data.entries or []

        callback? error, data




    #
    # callback (error, success)
    # 
    appendEntries: (message, callback) ->
        # Check if caller is out of date.
        if message.term < @currentTerm
            return callback? null, false

        previousEntry = @entryAt message.prevLogIndex

        # Check if we're out of date.
        if (@lastIndex < message.prevLogIndex) or (previousEntry?.term isnt message.prevLogTerm)
            return callback? null, false

        # TODO - convert to static
        newEntries = message.entries or { startIndex: 0, values: [] }

        # Check for heartbeats.
        if newEntries.length is 0
            @updateCommitIndex message.leaderCommit
            return callback? null, true

        # The currentTerm could only have changed if votedFor = 0 & votedFor can't
        # change in a term, so we can use it as a heuristic for when to write state.
        state = switch
            when @votedFor then {}
            else { currentTerm: @currentTerm, votedFor: 0 }

        await @storage.appendEntries newEntries.startIndex, newEntries.values, state, defer error

        if not error
            @entries.splice newEntries.startIndex
            @entries = @entries.concat newEntries.values
            @updateCommitIndex message.leaderCommit

        callback? error, not error


    
    #
    # callback (err, success)
    # 
    requestVote: (message, callback) ->
        # Check if caller is out of date.
        if message.term < @currentTerm
            return callback? null, false

        if not @votedFor or @votedFor is message.candidateId
            if (message.lastLogTerm > @lastTerm) or (message.lastLogTerm is @lastTerm and message.lastLogIndex >= @lastIndex)

                @votedFor = message.candidateId
                await @storage.set { @votedFor, @currentTerm }, defer error

                return callback? error, not error

        callback? null, false


    entriesSince: (index) ->
        return {
            startIndex: index + 1
            values: @entries.slice index + 1
        }


    updateCommitIndex: (index) ->
        if index > @commitIndex
            @commitIndex = Math.min index, @lastIndex
            @execute @commitIndex


    execute: (index, callback) ->
        return callback? 0 if index < @lastApplied

        if @lastApplied + 1 <= index
            for i in [@lastApplied + 1 .. index]
                await @executeEntry i, defer success

        callback? @lastApplied

    executeEntry: (index, callback) ->
        entry = @entryAt index
        return callback? null if entry.noop

        await @stateMachine.execute entry.op, defer error, result

        if not error        
            @lastApplied = index
            @emit "executed", index, entry, result
        else
            # TODO
            @emit "error", new Error "state machine error"

        callback? error, not error


module.exports = Log
