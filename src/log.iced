require "coffee-script-properties"
cuid = require "cuid"

class Log
    constructor: (@node, @options, doc) ->
        doc ?= 
            meta:
                lastIncludedIndex: 0
                lastIncludedTerm: 0
            entries: []

        @entries = doc.entries
        @lastIncludedIndex = doc.meta.lastIncludedIndex
        @lastIncludedTerm = doc.meta.lastIncludedTerm


    push: (entries...) ->
        for entry in entries
            entry.uuid = cuid()
            entry.index = @length + 1
            @entries.push entry
            @node.applyTopologyChange entry.command if entry.topologyChange


    pushEntries: (startIndex, entries) ->
         return if not entries or not entries.length

         @entries.splice startIndex - @lastIncludedIndex
         @push entry for entry in entries


    applied: (appliedIndex) ->
        toCut = appliedIndex - @lastIncludedIndex - @options.retainedLogEntries
        if toCut > 0
            cutAt = @entries[toCut - 1]
            @entries.splice 0, toCut
            @lastIncludedIndex += toCut
            @lastIncludedTerm = cutAt.term


    entryAt: (index) -> @entries[index - @lastIncludedIndex - 1]

    streamline: ->
        return {
            meta: { @lastIncludedIndex, @lastIncludedTerm }
            @entries
        }

    @getter "last", -> 
        if @entries.length
            return @entries[@entries.length - 1]
        else
            return undefined

    @getter "length", -> @lastIncludedIndex + @entries.length


module.exports = Log
