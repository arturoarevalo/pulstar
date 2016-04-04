Role = require "./role"
{PassThrough} = require "stream"

class Follower extends Role

    constructor: (node, @options) ->
        super node, "follower"

        @lastHeardFromLeader = undefined
        @lastLeader = undefined

        @node.startElectionTimeout()
        @node.state.persisted.votedFor = null

        @on "election timeout", @onElectionTimeout


    onElectionTimeout: =>
        if not @stopped
            @node.role = "candidate"


    onAppendEntries: (request, callback) ->
        log = @node.state.persisted.console.log 
        logEntry = null

        @node.startElectionTimeout()

        if request.leaderId
            @node.state.volatile.leaderId = request.leaderId

        if request.prevLogIndex
            logEntry = log.entryAt request.prevLogIndex
            if not logEntry and request.prevLogIndex is log.lastIncludedIndex and request.term is log.lastIncludedTerm
                logEntry = 
                    term: log.lastIncludedTerm

        cb = (success, message) =>
            response = 
                term: @node.currentTerm
                success: success
                lastApplied: @node.state.volatile.lastApplied

            response.reason = message if message

            callback null, response

        if request.term < @node.currentTerm
            return cb false, "term is < than current term"
        else if request.prevLogIndex and not logEntry
            return cb false, "local node too far behind"
        else if request.prevLogTerm and (not logEntry or logEntry.term isnt request.prevLogTerm)
            return cb false, "node too far behind"

        @lastLeader = request.leaderId
        @lastHeardFromLeader = Date.now()

        @node.state.persisted.currentTerm = request.term
        @node.state.volatile.commitIndex = Math.min request.leaderCommit, @node.state.persisted.log.length
        @node.state.persisted.log.pushEntries request.prevLogIndex, request.entries

        process.nextTick => 
            await @node.logApplier.persist defer error
            if error
                @emit "error", error
                callback error
            else
                cb true


    onRequestVote: (request, callback) ->
        minimumTimeout = @lastHeardFromLeader + @options.minElectionTimeout

        if @lastHeardFromLeader and minimumTimeout > Date.now()
            callback null,
                term: @node.currentTerm
                voteGranted: false
                reason: "too soon"
        else
            super request, callback


    onInstallSnapshot: (request, callback) ->
        calledback = false
        lastIncludedIndex = null
        lastIncludedTerm = null

        @node.startElectionTimeout()

        if request.term < @node.currentTerm
            return callback new Error "current term is #{@node.currentTerm}, not #{request.term}"

        if not @installingSnapshot and not request.first
            return callback new Error "expected first snapshot chunk: #{JSON.stringify request}"


        if request.lastIncludedIndex
            lastIncludedIndex = request.lastIncludedIndex

        if request.lastIncludedTerm
            lastIncludedTerm = request.lastIncludedTerm


        cb = (error) =>
            return if calledback

            calledback = true
            if error
                callback error
            else
                callback null,
                    term: @node.currentTerm

        if request.first
            await @node.options.storage.removeAllState @node.id, defer error

            if error
                callback error
            else
                @installingSnapshot = new PassThrough { objectMode: true }
                stream = @installingSnapshot.pipe @node.options.storage.createWriteStream @node.id
                stream.on "finish", =>
                    @installingSnapshot = false
                    @node.state.volatile.lastApplied = lastIncludedIndex
                    @node.state.volatile.commitIndex = lastIncludedIndex
                    @node.state.persisted.log.lastIncludedIndex = lastIncludedIndex
                    @node.state.persisted.log.lastIncludedTerm = lastIncludedTerm
                    @node.state.persisted.log.entries = []

                if request.data
                    @installingSnapshot.write request.data, cb

                if request.done
                    @installingSnapshot.end()
        
        else if request.data and @installingSnapshot
            @installingSnapshot.write request.data, cb

        else 
            cb()

        if request.done and not request.first
            @installingSnapshot.end request.data


module.exports = Follower
