### JSHINT ###
### global describe:true, it:true ###

Log = require "../src/log"
assert = require "assert"

describe "log", ->

    it "can be created from default values", (done) ->
        node = "NODE"
        options = "OPTIONS"

        log = new Log node, options

        assert.equal log.node, node
        assert.equal log.options, options
        assert.equal log.lastIncludedIndex, 0
        assert.equal log.lastIncludedTerm, 0
        assert.deepEqual log.entries, []

        done()

    
    it "can be created from a document", (done) ->
        doc =
            meta:
                lastIncludedIndex: 1
                lastIncludedTerm: 2
            entries: [1, 2, 3, 4, 5]

        log = new Log undefined, undefined, doc

        assert.equal log.lastIncludedIndex, 1
        assert.equal log.lastIncludedTerm, 2
        assert.deepEqual log.entries, [1, 2, 3, 4, 5]

        done()


    it "can push and get entries", (done) ->
        log = new Log

        log.push "a"
        log.push "b"
        log.push "c"

        assert.equal "a", log.entryAt 1
        assert.equal "b", log.entryAt 2
        assert.equal "c", log.entryAt 3
        assert.equal log.length, 3

        done()

    
    it "can push entries in bulk", (done) ->
        log = new Log

        log.pushEntries 0, ["a", "b", "c"]
        log.pushEntries 3, ["d", "e", "f"]

        assert.equal "a", log.entryAt 1
        assert.equal "b", log.entryAt 2
        assert.equal "c", log.entryAt 3
        assert.equal "d", log.entryAt 4
        assert.equal "e", log.entryAt 5
        assert.equal "f", log.entryAt 6
        assert.equal log.length, 6

        done()


    it "can push entries overriding existing ones", (done) ->
        log = new Log

        log.pushEntries 0, ["a", "b", "c"]
        log.pushEntries 2, ["c", "d", "e"]

        assert.equal "a", log.entryAt 1
        assert.equal "b", log.entryAt 2
        assert.equal "c", log.entryAt 3
        assert.equal "d", log.entryAt 4
        assert.equal "e", log.entryAt 5
        assert.equal log.length, 5

        done()


    it "does not compact the log before retained count", (done) ->
        N = 50
        log = new Log undefined, { retainedLogEntries: N }

        log.push i for i in [1 .. N]
        log.applied N for i in [1 .. N]

        assert.equal log.length, N
        assert.equal log.lastIncludedIndex, 0
        assert.equal log.lastIncludedTerm, 0
        assert.equal log.entries.length, N

        done()


    it "compacts the log after retained count is reached", (done) ->
        N = 50
        DELTA = 20
        N2 = 80
        log = new Log undefined, { retainedLogEntries: N }

        for i in [1 .. N2]
            log.push
                term: i
                command: i

        log.applied N + DELTA

        assert.equal log.length, N2
        assert.equal log.lastIncludedIndex, DELTA
        assert.equal log.lastIncludedTerm, DELTA
        assert.equal log.entries.length, N2 - DELTA

        done()
