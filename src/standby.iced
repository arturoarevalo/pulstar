module.exports = Standby

Role = require "role"

class Standby extends Role

    constructor: (node, @options) ->
        super node, "standby"
        