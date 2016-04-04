extend = require "xtend"
cuid = require "cuid"

DummyTransport = require "./dummy-transport"
DummyStorage = require "./dummy-storage"

Node = require "../src/node"


class DummyNode extends Node

    constructor: (options) ->
        opt = 
            id: cuid()
            transport: new DummyTransport
            storage: new DummyStorage

        super extend opt, options

        @once "stopped", =>
            @on "error", (error) -> console.error error


module.exports = DummyNode
