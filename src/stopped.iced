Role = require "./role"

class Stopped extends Role

    constructor: (node, @options) ->
        super node, "stopped"


    onAppendEntries: (request, callback) ->
        callback null,
            term: @node.currentTerm
            success: false
            lastApplied: @node.state.volatile.lastApplied
            reason: "node is stopped"


    onRequestVote: (request, callback) ->
        callback null,
            term: @node.currentTerm
            voteGranted: false
            reason: "node is stopped"


    onInstallSnapshot: (request, callback) ->
        callback new Error "node is stopped"
        

module.exports = Stopped
        