require "coffee-script-properties"
{EventEmitter} = require "events"

async = require "async"



class AsyncWorkQueue extends EventEmitter

    constructor: (@concurrency = 1, worker = null) ->
        @worker = worker or @processTask

        @queue =
            head: null
            tail: null
            length: 0

        @indices = []
        @slots = []

        for i in [0 .. @concurrency - 1]
            @indices[i] = -1
            @slots.push i

    @getter "waiting", -> @queue.length
    @getter "length", -> @queue.length
    @getter "running", -> @concurrency - @slots.length
    @getter "working", -> (item for item in @indices when item isnt null)

    push: (task, callback) ->
        tasklet = 
            task: task
            callback: callback
            next: null

        if @queue.tail
            @queue.tail.next = tasklet
            @queue.tail = tasklet

        if not @queue.head
            @queue.head = @queue.tail = tasklet

        @queue.length++
        @scheduleEventLoop()

    extractTasklet: ->
        item = @queue.head

        if @queue.head is @queue.tail
            @queue.head = @queue.tail = null
        else
            @queue.head = @queue.head.next

        @queue.length--
        return item

    scheduleEventLoop: =>
        if not @eventLoopScheduled
            @eventLoopScheduled = true
            process.nextTick @eventLoop

    eventLoop: => 
        @eventLoopScheduled = false
        while (@queue.head) and ((slot = @slots.pop()) isnt undefined)
            @runTask slot, @extractTasklet()

    runTask: (slot, tasklet) ->
        @indices[slot] = tasklet.task
        try
            @worker tasklet.task, (error, data) =>
                try
                    tasklet?.callback error, data
                catch cbex
                    console.error cbex

                @indices[slot] = null
                @slots.push slot
                @scheduleEventLoop()
        catch ex
            try
                tasklet?.callback error, data
            catch cbex
                console.error cbex

            @indices[slot] = null
            @slots.push slot
            @scheduleEventLoop()


    processTask: (task, callback) ->
        callback?()


class MQ extends AsyncWorkQueue

    processTask: (task, callback) ->
        console.log "hello #{task}"
        setTimeout callback, 500, null, task
        #callback null, task

#queue = new MQ 3

###
queue = new AsyncWorkQueue 3, (task, callback) ->
        console.log "hello #{task}"
        setTimeout callback, 500, null, task
        #callback null, task
###

###
w = (task, callback) ->
    console.log "hello #{task}"
    setTimeout callback, 500, null, task
    #callback null, task

queue = async.queue w, 3
#queue = new AsyncWorkQueue 

cb = (err, data) -> console.log "finished saying hello to #{data}"

queue.push "arturo", cb
queue.push "jorgia", cb
queue.push "luka", cb
queue.push "pequeño", cb
queue.push "kimi", cb
queue.push "hiro", cb
queue.push "muffi", cb
queue.push "pablo", cb
queue.push "chief", cb

setTimeout -> queue.push "nextone", cb
,1000
###



done = 0
calledBack = 0
TOTAL = 2000

cb = (err, data) -> 
    calledBack++
    if calledBack is TOTAL
        console.timeEnd "queue"
        console.log "finished! done=#{done} calledback=#{calledBack}"
        console.log queue.length
        console.log queue.running
        console.log queue.working

worker = (task, callback) ->
    done++
    callback()

queue = new AsyncWorkQueue 1, worker
#queue = async.queue worker, 1

console.time "queue"

for i in [1 .. TOTAL]
    queue.push i, cb

