extend = require "xtend"
cuid = require "cuid"

class Cluster

    DEFAULT_OPTIONS = {}

    constructor: (options) ->
        @options = extend {}, Cluster.DEFAULT_OPTIONS, options
        @id = @options.cluster or cuid()

module.exports = Cluster
