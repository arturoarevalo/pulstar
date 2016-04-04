Role = require "./role"

class Standby extends Role

    constructor: (node, @options) ->
        super node, "standby"
        

module.exports = Standby
        