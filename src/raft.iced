require "coffee-script-properties"

###
RAFT consensus algorithm as described in 
http://ramcloud.stanford.edu/raft.pdf
###    


class RaftAbstractStateMachine
    constructor: (id, options) ->
        @logger = options?.logger or require "winston-color"

    applyCommand: (command) ->
        @logger.info "applying state machine command", command

        return true



class RaftAbstractChannel
    constructor: (id, options) ->
        @logger = options?.logger or require "winston-color"

    executeRPC: (id, name, args) ->
        @logger.debug "RPC to", id, name



class RaftAbstractStorage
    constructor: (id, options) ->
        @logger = options?.logger or require "winston-color"

    save: (state, callback) -> callback? false

    load: (callback) -> callback? false



class RaftInMemoryChannel extends RaftAbstractChannel
    constructor: (id, options) ->
        super id, options


class RaftInMemoryStorage extends RaftAbstractStorage
    @store : {}

    constructor: (id, options) ->
        super id, options

    save: (state, callback) ->
        RaftInMemoryStorage.store[@id] = state
        @logger.debug "saved state", state
        console.log state
        callback? true

    load: (callback) ->
        data = RaftInMemoryStorage.store[@id]

        if data
            callback? true, data
        else
            callback? false




RaftStates = 
    LEADER: "leader"
    FOLLOWER: "follower"
    CANDIDATE: "candidate"


class Raft


    schedule: (fn, ms = 0, data = null) -> 
        cb = (d) => fn d
        setTimeout cb, ms, data
    
    unschedule: (id) -> clearTimeout id


    constructor: (id, options) ->
        @id = id
        @logger = options?.logger or require "winston-color"

        @storage = options?.storage or new RaftInMemoryStorage @id, { logger: @logger }
        @channel = options?.channel or new RaftInMemoryChannel @id, { logger: @logger }
        @stateMachine = options?.stateMachine or new RaftAbstractStateMachine @id, { logger: @logger }

        @options = 
            electionTimeout: 300
            heartbeatTime: 60
            firstServer: options?.firstServer or false
            serverData: { id: true }
            verbose: true


        # Persistent state - ALL SERVERS
        @currentTerm = 0
        @votedFor = null
        @log = [{ term: 0, command: null }]

        # Volatile data - ALL SERVERS
        @commitIndex = 0
        @lastApplied = 0
        @state = RaftStates.FOLLOWER
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
    updateTerm: (term = @currentTerm + 1) ->
        @currentTerm = term
        @votedFor = null
        @pendingPersist = true
        @votesResponded = {}
        @votesGranted = {}

    ###* 
     * Become a follower and start the election timeout timer.
    ###
    stepDown: ->
        return if @state is RaftStates.FOLLOWER

        @state = RaftStates.FOLLOWER
        @logger.info "new state #{@state}"

        # TODO - move
        @heartbeatTimer = @unschedule @heartbeatTimer if @heartbeatTimer

        @resetElectionTimer() if not @electionTimer


    ###*
     * Send a RPC to all other servers, excluding ourselves.
    ###
    sendRPCs: (rpc, args) ->
        for id in @servers when id isnt @id
            @channel.executeRPC id, rpc, args


    saveBefore: (callback) ->
        if @pendingPersist
            data = 
                currentTerm: @currentTerm
                votedFor: @votedFor
                log: @log

            @pendingPersist = false
            await @storage.save data, defer success

            console.log @serverMap

            @logger.error "Failed to persist state" if not success

        callback?()


    loadBefore: (callback) ->
        await @storage.load defer success, data

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
            @becomeLeader()

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
            entry.term = @currentTerm if "undefined" is typeof entry.term
            entry.command = null if "undefined" is typeof entry.command

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
                    @stateMachine.applyCommand command
                    status = "success"
                catch e
                    result = e.message
                    status = "error"


            # call client callback for the commited command
            callback = @clientCallbacks[@lastApplied]
            if callback
                callbacks[@lastApplied] = {callback, status, result}
                delete @clientCallbacks[@lastApplied]

            await @saveBefore defer error

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
        return if @state isnt RaftStates.LEADER

        for id in @servers when id isnt @id
            nindex = @nextIndex[id] - 1
            nterm = @log[nindex].term
            nentries = @log.slice nindex + 1

            if nentries.length
                @logger.debug "new entries to node #{id}", JSON.stringify nentries

            @channel.executeRPC id, "appendEntries",
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
        needed = Math.round (scnt + 1) / 2
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
        return if @state is RaftStates.LEADER

        @state = RaftStates.LEADER
        @logger.info "new state #{@state}"

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
        return if @state is RaftStates.LEADER

        @state = RaftStates.CANDIDATE
        @logger.info "new state #{@state}"

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
            return @sendRPC args.candidateId, "requestVoteResponse",
                term: @currentTerm
                voteGranted: false
                sourceId: @id

        if (@votedFor is null or @votedFor is args.candidateId) and
            (args.lastLogTerm >= @log[@log.length - 1].term or 
                (args.lastLogTerm is @log[@log.length - 1].term and args.lastLogTerm >= @log.length - 1))
            @votedFor = args.candidateId
            @pendingPersist = true
            @resetElectionTimer()
            await @saveBefore defer error
            return @sendRPC args.candidateId, "requestVoteResponse",
                term: @currentTerm
                voteGranted: true
                sourceId: @id

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
        if @checkVote @serverMap, @votesGranted
            @becomeLeader()


    appendEntries: (args) ->
        @logger.debug "rpc - appendEntries", JSON.stringify args

        # 1. reply false if term < currentTerm
        if args.term < @currentTerm
            await @saveBefore defer error
            return @sendRPC args.leaderId, "appendEntriesResponse",
                term: @currentTerm
                sucess: false
                sourceId: @id
                curAgreeIndex: @curAgreeIndex

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
            return @sendRPC args.leaderId, "appendEntriesResponse",
                term: @currentTerm
                sucess: false
                sourceId: @id
                curAgreeIndex: args.curAgreeIndex
            

        # 3. If existing entry conflicts with new entry (same index
        #    but different terms), delete the existing entry
        #    and all that follow. TODO: make this match the
        #    description.
        if args.prevLogIndex + 1 < @log.length
            @log.splice args.prevLogIndex + 1, @log.length

        # 4. append any new entries not already in the log
        if args.entries.length > 0
            @addEntries args.entries
            pendingPersist = true

        # 5. if leaderCommit > commitIndex, set
        #    commitIndex = min(leaderCommit, index of last new entry)
        if args.leaderCommit > @commitIndex
            @commitIndex = Math.min args.leaderCommit, @log.length - 1
            @applyEntries()

        await @saveBefore defer error
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



    addServers: (args, callback) ->
        @logger.debug "rpc - addServers", JSON.stringify args

        # 1. Reply NOT_LEADER if not leader (6.2)
        if @state isnt RaftStates.LEADER
            callback
                status: "NOT_LEADER"
                leaderHint: @leaderId
            return

        # NOTE: this is an addition to the Raft algorithm:
        # Instead of doing steps 2 and 3, just reject config
        # changes while an existing one is pending.
        # 2. Catch up new server for a fixed number of rounds. Reply
        #    TIMEOUT if new server does not make progress for an
        #    election timeout or if the last round takes longer than
        #    the election timeout (4.2.1)
        # 3. Wait until previous configuration in the log is
        #    committed (4.1)
        if @pendingConfigChange
            callback
                status: "PENDING_CONFIG_CHANGE"
                leaderHint: @leaderId
            return

        # NOTE: addition to Raft algorithm. If server is not in
        # serverData we cannot connect to it so reject it.
        if !(args.newServer of @options.serverData)
            callback
                status: "NO_CONNECTION_INFO"
                leaderHint: @leaderId
            return
    
        # NOTE: addition to Raft algorithm. If server is already
        # a member, reject it.
        if args.newServer of @options.serverMap
            callback
                status: "ALREADY_A_MEMBER"
                leaderHint: @leaderId
            return

        # 4. Append new configuration entry to the log (old
        #    configuration plus newServer), commit it using majority
        #    of new configuration (4.1)
        @pendingConfigChange = true
        @addEntries [ {
            oldServers: @servers
            newServer: args.newServer
            } ]

        @clientCallbacks[@log.length - 1] = ->
            @pendingConfigChange = false
            # 5. Reply OK
            callback
                status: 'OK'
                leaderHint: @leaderId
            return

        @updateIndexes()
        @pendingPersist = true

        # trigger leader heartbeat
        @schedule @leaderHeartbeat, 1


    addServerResponse: (args) ->
        @logger.debug "rpc - addServerResponse", JSON.stringify args



    removeServer: (args, callback) ->
        @logger.debug "rpc - removeServer", JSON.stringify args

        # 1. Reply NOT_LEADER if not leader (6.2)
        if @state isnt RaftStates.LEADER
            callback 
                status: "NOT_LEADER"
                leaderHint: @leaderId
            return

        # NOTE: this is an addition to the Raft algorithm:
        # Instead of doing step 2, just reject config changes while
        # an existing one is pending.
        # 2. Wait until previous configuration in the log is
        #    committed (4.1)
        if @pendingConfigChange
            callback
                status: 'PENDING_CONFIG_CHANGE'
                leaderHint: @leaderId
            return
        
        # NOTE: addition to Raft algorithm. If server is not in the
        # map, reject it.
        if !args.oldServer of @serverMap
            callback
                status: 'NOT_A_MEMBER'
                leaderHint: @leaderId
            return

        # 3. Append new configuration entry to the log (old
        #    configuration without oldServer), commit it using
        #    majority of new configuration (4.1)
        @pendingConfigChange = true
        @addEntries [ {
            oldServers: @servers
            oldServer: args.oldServer
        } ]

        @clientCallbacks[@log.length - 1] = ->
            @pendingConfigChange = false
            # 4. Reply OK, and if this server was removed, step down
            #    (4.2.2)
            # TODO: step down and terminate
            callback
                status: 'OK'
                leaderHint: @leaderId
            return

        @updateIndexes()
        @pendingPersist = true

        # trigger leader heartbeat
        @schedule @leaderHeartbeat, 1


    removeServerResponse: (args) ->
        @logger.debug "rpc - removeServerResponse", JSON.stringify args


    clientRequest: (cmd) ->
        callback = (args) ->
            @sendRPC cmd.responseId, "clientRequestResponse", args if cmd.responseId

        if @state isnt RaftStates.LEADER
            callback 
                status: "NOT_LEADER"
                leaderHint: @leaderId
            return

        # NOTE: this is an addition to the basic Raft algorithm:
        # Read-only operations are applied immediately against the
        # current state of the stateMachine (i.e. committed state)
        # and are not added to the log. Otherwise, the cmd is added
        # to the log and the client callback will be called when the
        # cmd is is committed. See 8
        tcmd = @copyMap cmd
        delete tcmd.responseId
        if tcmd.ro
            status = null
            try
                result = @stateMachine.applyCommand cmd
                status = 'success'
            catch exc
                result = exc
                status = 'error'
            callback
                status: status
                result: result
        else
            @clientCallbacks[@log.length] = callback
            @addEntries [ { command: tcmd } ]
            @pendingPersist = true

            # trigger leader heartbeat
            @schedule @leaderHeartbeat, 1


    clientRequestResponse: (args) ->
        @logger.debug "rpc - clientRequestResponse", JSON.stringify args




    initialize: ->
        @schedule =>
            @logger.info "initializing"
            await @loadBefore defer error
            @logger.info "initialized"


    copyMap: (obj) ->
        nobj = {}
        nobj[key] = value for key, value of obj
        return nobj


module.exports = Raft
