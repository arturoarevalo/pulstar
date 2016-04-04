### JSHINT ###
### global describe:true, it:true ###

Log = require "../src/log"
Peer = require "../src/peer"

DummyTransport = require "./dummy-transport"
DummyConnection = require "./dummy-connection"

assert = require "assert"


describe "peer", ->

    it "can be created and retains options", (done) ->
        node = { id: "test-node" }
        peer = new Peer "test-peer", "sample metadata", { transport: new DummyTransport }, undefined, node

        assert.equal peer.id, "test-peer"
        assert.equal peer.metadata, "sample metadata"

        done()


    it "connects and passes right metadata", (done) ->
        node = { id: "node-id", metadata: "node-metadata"}
        transport = new DummyTransport
        peer = new Peer "peer-id", "peer-metadata", { transport }, undefined, node

        transport.on "connect", (nodeId, localMetadata, id, metadata) ->
            assert.equal nodeId, "node-id"
            assert.equal localMetadata, "node-metadata"
            assert.equal id, "peer-id"
            assert.equal metadata, "peer-metadata"

            done()

        peer.connect()


    it "can not send messages before it is connected", (done) ->
        node = { id: "node-id", metadata: "node-metadata"}
        transport = new DummyTransport
        peer = new Peer "peer-id", "peer-metadata", { transport }, undefined, node

        peer.send "message", "parameters", (error) ->
            assert error instanceof Error
            done()


    it "can make remote procedure calls", (done) ->
        sent = false

        node = { id: "node-id", metadata: "node-metadata"}
        transport = new DummyTransport
        peer = new Peer "peer-id", "peer-metadata", { transport }, undefined, node
        connection = peer.connect()

        connection.on "send", (id, message, parameters) ->
            assert.equal message, "message"
            assert.equal parameters, "parameters"
            sent = true

        peer.send "message", "parameters", (error, result) ->
            assert sent
            done()


    it "serializes and replies to remote calls", (done) ->
        node = { id: "node-id", metadata: "node-metadata"}
        transport = new DummyTransport
        peer = new Peer "peer-id", "peer-metadata", { transport }, undefined, node

        current = 1
        expected = 1

        transport.listen "test-id", "peer-id", (message, parameters, callback) ->
            assert.equal message, "message-#{expected}"
            assert.equal parameters, "parameters-#{expected}"

            cb = ->
                reply = "reply-#{expected}"
                expected++
                callback null, reply 

            setTimeout cb, 5

        peer.connect()

        sendCallback = (error, result) ->
            throw error if error
            assert.equal result, "reply-#{current}"
            current++
            done() if current is 11

        for i in [1 .. 10] 
            peer.send "message-#{i}", "parameters-#{i}", sendCallback

