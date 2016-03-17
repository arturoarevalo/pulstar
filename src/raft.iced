require "coffee-script-properties"

###
RAFT consesus algorithm as described in 
http://ramcloud.stanford.edu/raft.pdf
###    

class Raft


    schedule: (fn, ms, data) -> setTimeout fn, ms
    
    unschedule: (id) -> clearTimeout id


    saveState: (data, callback) ->
        callback? false

    loadState: (callback) ->
        callback? false


    constructor: (id, options) ->

        @options = 
            electionTimeout: 300
            heartbeatTime: 60
            stateMachineStart: {}
            firstServer: false
            serverData: { id: true }
            verbose: true
            logger: (args...) -> console.log args...


        # Persistent state - ALL SERVERS
        @currentTerm = 0
        @votedFor = null
        @log = [{ term: 0, command: null }]

        # Volatile data - ALL SERVERS
        @commitIndex = 0
        @lastApplied = 0
        @state = "follower"
        @stateMachine = @options.stateMachineStart
        @serverMap = {}
        
        # Volatile data - LEADERS
        @nextIndex = {}
        @matchIndex = {}

        # Volatile data - CANDIDATES
        @votesResponded = {}
        @votesGranted = {}


        # Private data
        @electionTimer = null
        @heartbeatTimer = null
        @leaderId = null
        @clientCallbacks = {}
        @pendingPersist = false
        @pendingConfigChange = false


    ###*
     * Returns a list will all server IDs.
    ###
    @getter "servers", -> Object.keys @serverMap


    ###*
     * Clear or cancel any existing election timer.
    ###
    clearElectionTimer: -> @electionTimer = @unschedule @electionTimer if @electionTimer


    ###*
     * Reset the election timer to a random value in the range [electionTimeout, electionTimeout * 2].
    ###
    resetElectionTimer: -> 
        timeout = @options.electionTimeout + parseInt Math.random() * @options.electionTimeout
        @clearElectionTimer()
        @electionTimer = @schedule @startElection, timeout


    ###*
     * Set our term value.
    ###
    updateTerm: (term) ->
        term = @currentTerm + 1 if "undefined" is typeof term
        @currentTerm = term
        @votedFor = null
        @pendingPersist = true
        @votesResponded = {}
        @votesGranted = {}

    ###* 
     * Become a follower and start the election timeout timer.
    ###
    stepDown: ->
        return if @state is "follower"
        @state = "follower"

        # TODO - move
        @heartbeatTimer = @unschedule @heartbeatTimer if @heartbeatTimer

        @resetElectionTimer() if not @electionTimer


    ###*
     * Send a RPC to all other servers, excluding ourselves.
    ###
    sendRPCs: (rpc, args) ->
        for id in @servers when id isnt @id
            @sendRPC id, rpc, args


    saveBefore: (callback) ->
        if @pendingPersist
            data = 
                currentTerm: @currentTerm
                votedFor: @votedFor
                log: @log

            @pendingPersist = false
            await @saveState data, defer success

            @logger.error "Failed to persist state" if not success

        callback?()


    loadBefore: (callback) ->
        await @loadState defer success, data

        if success and @options.firstServer
            @options.firstServer = false
            @logger.error "This node is not the first server"

        if success
            # update state from loaded data
            @currentTerm = data.currentTerm
            @votedFor = data.votedFor
            @addEntries data.log, true

            @logger.info "Loaded persistent state, starting election timer"
            @stepDown()
            @resetElectionTimer()
        
        else if @options.firstServer
            # if no data was loaded but we're the first server then start ourselves as the only member
            @currentTerm = 0
            @votedFor = null
            @addEntries [{ newServer: @id, oldServers: [] }], true
            @logger.info "This is the first server, assuming leadership"

        else
            # if no data was loaded and we're not the first server, we'll have an empty log
            @currentTerm = -1
            @voterFor = null
            @logger.info "This isn't the first server, waiting for the initial RPC"
            @clearElectionTimer()

        @votesResponded = {}
        @votesGranted = {}
        callback?()



    addEntries: (entries, startup) ->
        for entry in entries
            entry.term = @currentTerm if "undefined" is typeof @entry.term
            entry.command = null if "undefined" is typeof @entry.command

            if entry.newServer
                @logger.debug "adding new server #{entry.newServer}"
                @serverMap[entry.newServer] = @options.serverData[entry.newServer]
            else if entry.oldServer
                @logger.debug "removing old server #{entry.oldServer}"
                delete @serverMap[entry.oldServer]

            @log.push entry

        # TODO: check that all entries in serverMap have connection information in options.serverData


    ###*
     * Apply log entries from self.lastApplied up to self.commitIndex by calling 
     * options.applyCmd on the current state of self.stateMachine.
     * 
     * Figure 2, Rules for Servers, All Servers
    ###
    applyEntries: ->
        while @commitIndex > @lastApplied
            @lastApplied++

            entry = @log[@lastApplied]
            command = entry.command
            callbacks = {}
            status = null
            result = null

            if command
                @logger.debug "applying command #{command}"

                try
                    @applyCommand @stateMachine, command
                    status = "success"
                catch e
                    result = e.message
                    status = "error"


            # call client callback for the commited command
            callback = @clientCallbacks[@lastApplied]
            if callback
                callbacks[@lastApplied] = {callback, status, result}
                delete @clientCallbacks[@lastApplied]

            await @saveBefore

            for cb in callbacks
                cb.callback? cb.status, cb.result



    getMajorityIndex: (ids) ->
        agreeIndexes = [@log.length - 1]
        for id in ids when id isnt @id
            agreeIndexes.push @matchIndex[id]

        agreeIndexes.sort()
        agreePos = Math.floor ids.length / 2
        majorityIndex = agreeIndexes[agreePos]

        return majorityIndex



    checkCommits: ->
        majorityIndex = @getMajorityIndex @servers

        if majorityIndex > @commitIndex
            termStored = false
            for i in [majorityIndex .. @log.length]
                if @log[i].term is @currentTerm
                    termStored = true
                    break

            if termStored
                @commitIndex = Math.min majorityIndex, @log.length - 1
                @applyEntries()




    leaderHeartbeat: ->
        return if @state isnt "leader"

        for id in @servers when id isnt @id
            nindex = @nextIndex[id] - 1
            nterm = @log[nindex].term
            nentries = @log.slice nindex + 1

            if nentries.length
                @logger.debug "new entries to node #{id}", JSON.stringify nentries

            @sendRPC id, "appendEntries",
                term: @currentTerm
                leaderId: @id
                prevLogIndex: nindex
                prevLogTerm: nterm
                entries: nentries
                leaderCommit: @commitIndex
                curAgreeIndex: @log.length - 1

        @unschedule @heartbeatTimer
        @heartbeatTimer = @schedule @leaderHeartbeat, @options.heartbeatTime

        @checkCommits()                



    checkVote: (serverMap, voteMap) ->
        scnt = @servers.length
        need = Math.round (scnt + 1) / 2
        votes = {}

        for k in serverMap
            votes[k] = true if "undefined" isnt typeof voteMap[k]

        if Object.keys(votes).length >= needed
            return true
        else
            return false



    updateIndexes: (fromScratch) ->
        if fromScratch
            @nextIndex = {}
            @matchIndex = {}

        for id in @servers
            if "undefined" is typeof @nextIndex[id]
                @nextIndex[id] = @log.length
            if "undefined" is typeof @matchIndex[id]
                @matchIndex[id] = @commitIndex


    becomeLeader: ->
        return if @state is "leader"

        @logger.info "new state 'leader'"

        @state = "leader"
        @leaderId = @id
        @votesResponded = {}
        @votesGranted = {}
        @votedFor = null
        @pendingPersist = true

        # ADDITION
        @addEntries [{ newLeaderId: @id }]

        @updateIndexes true
        @clearElectionTimer()
        @saveBefore @leaderHeartbeat




    # Section 3.4
    startElection: ->
        # leaders can't start the election process
        return if @state is "leader"

        @logger.info "new state 'candidate'"

        @state = "candidate"
        @updateTerm()

        @votedFor = @id
        @votesGranted[@id] = true
        @pendingPersist = true
        @resetElectionTimer()

        @sendRPCs "requestVote",
            term: @currentTerm
            candidateId: @id
            lastLogIndex: @log.length - 1
            lastLogTerm: @log[@log.length - 1].term


    terminate: ->
        @logger.info "terminating"

        @heartbeatTimer = @unschedule @heartbeatTimer if @heartbeatTimer
        @clearElectionTimer()

        # TODO
        # disable API.requestVote & API.appendEntries



    #
    # RPC / API
    # 
    
    requestVote: (args) ->
        @logger.debug "rpc - requestVote", JSON.stringify args

        if args.term > @currentTerm
            @updateTerm args.term
            @stepDown()

        if args.term < @currentTerm
            await @saveBefore defer error
            @sendRPC args.candidateId, "requestVoteResponse",
                term: @currentTerm
                voteGranted: false
                sourceId: @id

            return

        if (@votedFor is null or @votedFor is args.candidateId) and
            (args.lastLogTerm >= @log[@log.length - 1].term or 
                (args.lastLogTerm is @log[@log.length - 1].term and args.lastLogTerm >= @log.length - 1))
            @votedFor = args.candidateId
            @pendingPersist = true
            @resetElectionTimer()
            await @saveBefore defer error
            @sendRPC args.candidateId, "requestVoteResponse",
                term: @currentTerm
                voteGranted: true
                sourceId: @id
            return

        await @saveBefore defer error
        @sendRPC args.candidateId, "requestVoteResponse",
            term: @currentTerm
            voteGranted: false
            sourceId: @id


    requestVoteResponse: (args) ->
        @logger.debug "rpc - requestVoteResponse", JSON.stringify args

        if args.term > @currentTerm
            @updateTerm args.term
            @stepDown()
            return

        otherId = args.sourceId

        # ignore if we're not a candidate
        if @state isnt "cantidate" or args.term < @currentTerm
            return

        if args.voteGranted
            @logger.debug "got a vote from", otherId
            @votesGranted[otherId] = true

        @logger.debug "current votes", Object.keys @votesGranted

        # check if we won the election
        if @checkVote @serverMap, votesGranted
            @becomeLeader()


    appendEntries: (args) ->
        @logger.debug "rpc - appendEntries", JSON.stringify args

        # 1. reply false if term < currentTerm
        if args.term < @currentTerm
            await @saveBefore defer error
            @sendRPC args.leaderId, "appendEntriesResponse",
                term: @currentTerm
                sucess: false
                sourceId: @id
                curAgreeIndex: @curAgreeIndex
            return

        # step down if we're candidate or leader
        @stepDown()
        if @leaderId isnt args.leaderId
            @leaderId = args.leaderId
            @logger.info "new leader", @leaderId

        @resetElectionTimer()

        # TODO
        # if we've pending clientCallbacks, it means that we were a leader
        # but lost our position ... reject the clientCallbacks with a 
        # "not_leader" response
        
        # 2. reply false if log doesn't contain any entry at prevLogIndex whose
        #    term matches prevLogTerm 
        if (@log.length - 1 < args.prevLogIndex) or (@log[args.prevLogIndex].term isnt args.prevLogTerm)
            await @saveBefore defer error
            @sendRPC args.leaderId, "appendEntriesResponse",
                term: @currentTerm
                sucess: false
                sourceId: @id
                curAgreeIndex: args.curAgreeIndex
            return

        # 3. If existing entry conflicts with new entry (same index
        #    but different terms), delete the existing entry
        #    and all that follow. TODO: make this match the
        #    description.
        if args.prevLogIndex + 1 < @log.length
            @log.splice args.prevLogIndex + 1, @log.length

        # 4. append any new entries not already in the log
        if args.entries.length > 0
            addEntries args.entries
            pendingPersist = true

        # 5. if leaderCommit > commitIndex, set
        #    commitIndex = min(leaderCommit, index of last new entry)
        if args.leaderCommit > @commitIndex
            @commitIndex = Math.min args.leaderCommit, @log.length - 1
            @applyEntries()

        await saveBefore defer error
        @sendRPC args.leaderId, "appendEntriesResponse",
            term: @currentTerm
            success: true
            sourceId: @id
            curAgreeIndex: args.curAgreeIndex



    appendEntriesResponse: (args) ->
        @logger.debug "rpc - appendEntriesResponse", JSON.stringify args

        # shouldn't happen ...
        if args.term > @currentTerm
            @updateTerm args.term
            @stepDown()
            return

        sid = args.sourceId
        if args.success
            # The log entry is considered commited if it's stored on a
            # majority of nodes. Also, at least one entry from the leader's
            # must also be stored on a majority of nodes.
            @matchIndex[sid] = args.curAgreeIndex
            @nextIndex[sid] = args.curAgreeIndex + 1
            @checkCommits()
        else
            @nextIndex[sid]--
            if @nextIndex[sid] is 0
                # The first log entry is always the same, so start with the
                # second (setting nextIndex[sid] to 0 results in occasional
                # errors from -1 indexing into the log).
                @logger.debug "forcing nextIndex[#{sid}] to 1"
                @nextIndex[sid] = 1

            # TODO - Resend immediately






module.exports = Raft
