
RaftServer = require "../src/raft/raft-server"

MemoryStateMachine = require "../src/raft/memory-state-machine"
MemoryStorage = require "../src/raft/memory-storage"


storage1 = new MemoryStorage
machine1 = new MemoryStateMachine

storage2 = new MemoryStorage
machine2 = new MemoryStateMachine

storage3 = new MemoryStorage
machine3 = new MemoryStateMachine

r1 = new RaftServer "raft1", storage1, machine1
r2 = new RaftServer "raft2", storage2, machine2
r3 = new RaftServer "raft3", storage3, machine3

r1.start [r1, r2, r3], "follower"
r2.start [r1, r2, r3], "follower"
r3.start [r1, r2, r3], "follower"

