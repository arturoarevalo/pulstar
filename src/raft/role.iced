{EventEmitter} = require "events"

class Role extends EventEmitter

    constructor: (@log, @state, options) ->
        @logger = options?.logger or require "winston-color"

        @electionTimeout = options?.electionTimeout or 200
        @electionTimer = null

    clearElectionTimeout: ->
        if @electionTimer
            clearTimeout @electionTimer
            @electionTimer = null

    resetElectionTimeout: ->
        @clearElectionTimeout()
        @electionTimer = setTimeout @beginElection, @electionTimeout + Math.random() * @electionTimeout


    appendEntries: (message, callback) ->
        callback? null, false

    entriesAppended: (message, callback) ->
        callback? null, false

    requestVote: (vote, callback) ->
        callback? null, false


module.exports = Role
