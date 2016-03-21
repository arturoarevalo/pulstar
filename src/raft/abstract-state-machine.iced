class AbstractStateMachine
    constructor: (options) ->
        @logger = options?.logger or require "winston-color"

    execute: (operation, callback) ->
        callback? { message: "NOT IMPLEMENTED" }


module.exports = AbstractStateMachine
