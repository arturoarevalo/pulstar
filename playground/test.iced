
Raft = require "../src/raft"



instance = new Raft "machine-1", { firstServer : true }

instance.initialize()
