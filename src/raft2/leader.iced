Role = require "role"
Replicator = require "replicator"

class Leader extends Role

    constructor: (node, @options) ->
        super node, "leader"



module.exports = Leader
