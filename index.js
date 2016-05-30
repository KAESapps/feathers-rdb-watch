'use strict';
var addFilters = require('feathers-rethinkdb/lib/parse')

const rdbChangesQuery = (r, table, params) => {
  var sort = {index: 'id'}
  if (params.$sort) {
    const sortField = Object.keys(params.$sort)[0]
    sort.index = params.$sort[sortField] === -1 ? r.desc(sortField) : sortField
    // console.log('watch sort', sort)
  }
  var skip = params.$skip || 0
  var limit = params.$limit || 100
  var filter = Object.assign({}, params)
  delete filter.$sort
  delete filter.$limit
  delete filter.$skip
  delete filter.$select
  // console.log('watch filter', filter)

  var query = table.orderBy(sort)
  query = addFilters({options: {r}}, query, filter)
  return query
    .pluck('id')
    // .skip(skip) // does not work actually (https://github.com/rethinkdb/rethinkdb/issues/4909)
    .limit(skip + limit) // since skip does not work, we watch from the beginning
    .changes({
      squash: true,
      includeInitial: false,
    })
}

// TODO: don't use a class
module.exports = class SubscriptionsService {
  constructor(arg) {
    this.events = ['change'];
    this._subscriptions = {}
    this._disconnectListeners = {}
    this._db = arg.db
    this._serviceName = arg.service // TODO: allow to inject the service directly
    this._tableName = arg.table
  }

  setup(app, path) {
    this._service = app.service(this._serviceName)
    this._path = path
  }

  create(data, params) {
    const queryParams = data.params
    const path = this._path
    const subscriptions = this._subscriptions
    const service = this._service
    const socket = params.socket
    const clientId = socket.id
    const subscriptionId = clientId+'::'+data.id
    const query = data.type === 'query' ?
      rdbChangesQuery(this._db, this._db.table(this._tableName), queryParams) :
      this._db.table(this._tableName).get(queryParams).changes()
    return query.run().then(cursor => {
      //close cursor when client disconnect
      var disconnectListener = () => this.remove(data.id, params).catch(console.error.bind(console, 'error deleting subscription'))
      this._disconnectListeners[subscriptionId] = disconnectListener
      socket.on('disconnect', disconnectListener)
      console.log(this._serviceName, 'subscription created', data.id)
      subscriptions[subscriptionId] = cursor
      var req = data.type === 'query' ?
        service.find({query: Object.assign({}, queryParams)}): // needs to send a copy since qyueryParams is mutated by the called code
        service.get(queryParams)
      // init result
      req.then(result => {
        socket.emit(path+' change', {key: data.id, type: data.type, value: result})
      }).catch(err => console.warn('error getting first value for key', data.id, err))
      // next results
      cursor.each((err, change) => {
        if (err) return console.error(err)
        console.log(this._serviceName, 'changed', data.id, change)
        req = data.type === 'query' ?
          service.find({query: Object.assign({}, queryParams)}) :
          service.get(queryParams)
        req.then(result => {
          socket.emit(path+' change', {key: data.id, type: data.type, params: queryParams, value: result})
        }).catch(err => console.warn('error getting value for key', data.id, err))
      })
    })
  }

  remove(id, params) {
    const clientId = params.socket.id
    const subscriptionId = clientId+'::'+id

    // unsubscribe from rethinkdb change feed
    const cursor = this._subscriptions[subscriptionId]
    cursor.close()
    console.log(this._serviceName, 'subscription deleted', subscriptionId)
    delete this._subscriptions[subscriptionId]
    // remove disconnect listener
    params.socket.removeListener('disconnect', this._disconnectListeners[subscriptionId])
    delete this._disconnectListeners[subscriptionId]

    return Promise.resolve({id: id, closed: true})
  }
}
