
RS = require "../src/raft/raft-server"

MemoryStateMachine = require "../src/raft/memory-state-machine"
MemoryStorage = require "../src/raft/memory-storage"

{EventEmitter} = require "events"











class Logger
    constructor: (@id) ->
        @logger = require "winston-color"

    debug: (args...) ->
        @logger.debug "[#{@id}]", args...

    info: (args...) ->
        @logger.info "[#{@id}]", args...

    error: (args...) ->
        @logger.error "[#{@id}]", args...


logger1 = new Logger "raft1"
logger2 = new Logger "raft2"
logger3 = new Logger "raft3"


storage1 = new MemoryStorage
machine1 = new MemoryStateMachine { logger: logger1 }

storage2 = new MemoryStorage
machine2 = new MemoryStateMachine { logger: logger2 }

storage3 = new MemoryStorage
machine3 = new MemoryStateMachine { logger: logger3 }

r1 = new RS "raft1", storage1, machine1, { logger: logger1 }
r2 = new RS "raft2", storage2, machine2, { logger: logger2 }
r3 = new RS "raft3", storage3, machine3, { logger: logger3 }

r1.start [r1, r2, r3], "follower"
r2.start [r1, r2, r3], "follower"
r3.start [r1, r2, r3], "follower"

findLeader = -> switch
    when r1.role.state is "leader" then r1
    when r2.role.state is "leader" then r2
    when r3.role.state is "leader" then r3
    else null

cb = -> 
    r = findLeader()
    if r
        console.log "client requests"
        error = {}
        await for i in [1..5]
            r.request { nanana: "nenene" }, defer error[i]
        console.log "server responses", error
    else
        console.log "no leader"

setTimeout cb, 5000
#setTimeout cb, 4000
#setTimeout cb, 5000



