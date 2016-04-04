module.exports = Idle

Follower = require "./follower"

class Idle extends Follower

    constructor: (node, options) ->
        super node, options
        @name = "idle"
