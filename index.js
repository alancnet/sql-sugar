const Session = require('./session')
const Table = require('./table')
const nestLiteral = require('nest-literal')


const create = opts => new Session(opts)

module.exports.create = create
module.exports.nest = nestLiteral.nest
module.exports.raw = nestLiteral.raw
module.exports.Session = Session
module.exports.Table = Table