AbstractStateMachine = require "./abstract-state-machine"

class MemoryStateMachine extends AbstractStateMachine
    constructor: (options) ->
        super options

        @state = 1

    execute: (operation, callback) ->
        @state++
        callback? null, state


module.exports = MemoryStateMachine
