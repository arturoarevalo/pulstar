require "coffee-script-properties"
{EventEmitter} = require "events"



class AsyncWorkQueue extends EventEmitter

    constructor: (@concurrency = 1) ->
        @queue =
            head: null
            tail: null
            waiting: 0
            running: 0

    @getter "waiting", -> @queue.waiting
    @getter "length", -> @queue.waiting
    @getter "running", -> @queue.running


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

        @queue.waiting++
        @eventLoop()


    extractTasklet: ->
        available = @concurrency - @running
        if available and @queue.head
            item =
                task: @queue.head.task
                callback: @queue.head.callback

            if @queue.head is @queue.tail
                @queue.head = @queue.tail = null
            else
                @queue.head = @queue.head.next

            @queue.waiting--
            return item

        return null


    eventLoop: => @runTask tasklet while (tasklet = @extractTasklet()) isnt null

    runTask: (tasklet) ->
        @queue.running++
        @processTask tasklet.task, (error, args...) =>
            @queue.running--
            process.nextTick @eventLoop

            tasklet.callback? error, args...


    processTask: (task, callback) ->
        callback?()






class MQ extends AsyncWorkQueue

    processTask: (task, callback) ->
        console.log "hello #{task}"
        setTimeout callback, 1000, null, task
        #callback null, task

queue = new MQ 5


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


setTimeout ->
    queue.push "nextone", cb
, 10000


console.log "finished queuing"


