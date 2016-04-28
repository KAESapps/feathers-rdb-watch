'use strict';

const rdbChangesQuery = (table, params) => {
  var sort = params.$sort ? {index: params.$sort} : {index: 'id'}
  var limit = params.$limit || 100
  var skip = params.$skip
  var filter = Object.assign({}, params)
  delete filter.$sort
  delete filter.$limit
  delete filter.$skip

  return table
    .orderBy(sort)
    .filter(filter)
    .pluck('id')
    // .skip(data.skip || 0)
    .limit(limit)
    .changes({
      // squash: true,
      // includeInitial: true,
      // includeStates: true,
      // includeOffsets: true,
      // includeTypes: true,
    })
}


module.exports = class SubscriptionsService {
  constructor(arg) {
    this.events = ['change'];
    this._subscriptions = {}
    this._db = arg.db
    this._serviceName = arg.service
    this._tableName = arg.table
  }

  setup(app, path) {
    this._service = app.service(this._serviceName)
    this._path = path
  }

  create(data, params) {
    const that = this
    const path = this._path
    const subscriptions = this._subscriptions
    const service = this._service
    const socket = params.socket
    const clientId = socket.id
    const subscriptionId = clientId+'::'+data.id
    const query = data.type === 'query' ?
      rdbChangesQuery(this._db.table(this._tableName), data.params) :
      this._db.table(this._tableName).get(data.params).changes()
    return query.run().then(cursor => {
      //à la déconnexion du client, fermer le curseur
      socket.on('disconnect', () =>
        that.remove(data.id, params).catch(console.error.bind(console, 'error deleting subscription'))
      )
      console.log(this._serviceName, 'subscription created', data.id)
      subscriptions[subscriptionId] = {cursor: cursor, socket: params.socket}
      var req = data.type === 'query' ?
        service.find({query: data.params}):
        service.get(data.params)
      // init result
      req.then(result => {
        socket.emit(path+' change', {key: data.id, type: data.type, value: result})
      }).catch(err => console.warn('error getting first value for key', data.id, err))
      // next results
      cursor.each((err, change) => {
        if (err) return console.error(err)
        console.log(this._serviceName, 'changed', data.id /*,change*/)
        req = data.type === 'query' ?
          service.find({query: data.params}) :
          service.get(data.params)
        req.then(result => {
          socket.emit(path+' change', {key: data.id, type: data.type, params: data.params, value: result})
        }).catch(err => console.warn('error getting value for key', data.id, err))
      })
    })
  }

  remove(id, params) {
    const clientId = params.socket.id
    const subscriptionId = clientId+'::'+id

    const cursor = this._subscriptions[subscriptionId].cursor
    cursor.close()
    console.log(this._serviceName, 'subscription deleted', subscriptionId)
    delete this._subscriptions[subscriptionId]
    return Promise.resolve({id: id, closed: true})
  }
}
