require "coffee-script-properties"

###
RAFT consesus algorithm as described in 
http://ramcloud.stanford.edu/raft.pdf
###    

class Raft

    constructor: (id, options) ->

        @options = 
            electionTimeout: 300
            heartbeatTime: 60
            stateMachineStart: {}
            firstServer: false
            serverData: { id: true }
            verbose: true
            logger: (args...) -> console.log args...
            schedule: (fn, ms, data) -> setTimeout fn, ms
            unschedule: (id) -> clearTimeout id
            saveFn: (data, callback) -> callback? false
            loadFn: (callback) -> callback? false


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


    
        







module.exports = Raft
