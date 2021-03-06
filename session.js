const sql = require('mssql')
const EventEmitter = require('events')
const { nest } = require('nest-literal')
const { sqlifyTemplate, trimParameters } = require('./util')
const Table = require('./table')

const restack = (stack1, stack2) => [
  ...stack1.split('\n').filter(x => !x.startsWith('    at ')),
  ...stack2.split('\n').slice(2).filter(x => !x.includes('sql-sugar'))
].join('\n')

class Session extends EventEmitter {
  constructor(config) {
    super()
    const stack = new Error().stack
    if (!config.transaction) {
      this.pools = Promise.resolve(config).then(config => {
        this.configs = config.database.split(',').map(database => ({ ...config, database }))
        return Promise.all(this.configs.map(config => {
          const pool = new sql.ConnectionPool(config).connect()
          pool.catch(err => {
            this.emit('error', err)
          })
          return pool.catch(err => {
            err.stack = restack(err.stack, stack)
            throw err
          })
        }))
      })
      this.pool = this.pools.then(pools => pools.slice().pop())
    } else {
      this.transaction = config.transaction
      this.parent = config.parent
    }
    for (const fn of ['sql', 'request', 'transaction', 'close']) {
      this[fn] = this[fn].bind(this)
    }
    this.class = this.table // Alias
  }
  
  async request() {
    if (this.pool) return (await this.pool).request()
    else if (this.transaction) return this.transaction.request()
  }
  
  sql(callSite, ...substitutions) {
    if (typeof callSite === 'string') throw new Error('Do not call sql as a function. Call as a string template to avoid SQL injection.')
    const template = nest(callSite, ...substitutions)
    
    template.then = (resolve, reject) => (async () => {
      const stack = new Error().stack
      const request = await this.request()
      const trimmedTemplate = trimParameters(template)
      this.emit('sql-template', template)
      try {
        return await request.query(...trimmedTemplate)
      } catch (err) {
        err.stack = restack(err.stack, stack)
        err.sql = template.toString()
        err.template = template
        throw err
      }
    })().then(resolve, reject)
    
    template.toString = function() {
      return sqlifyTemplate(this)
    }

    template.recordset = () => template.then(result => result.recordset || null)
    template.one = () => template.then(result => result.recordset[0] || null)
    
    return template
  }
  async transaction(fn) {
    const transaction = new sql.Transaction(await (this.pool || this.transaction))
    return new Promise((resolve, reject) => {
      transaction.begin(async err => {
        let rolledBack = false
        transaction.on('rollback', () => {
          rolledBack = true
        })
        if (err) return reject(err)
        let result
        try {
          const transSession = new Session({transaction, parent: this})
          result = await fn(transSession)
          await transaction.commit()
          resolve(result)
        } catch (userError) {
          if (!rolledBack) {
            try {
              await transaction.rollback()
              reject(Object.assign(new Error(
                `Transaction error, manually rolled back: ${userError}`
              ), { userError }))
            } catch (rollbackError) {
              reject(Object.assign(new Error(
                `SEVERE ERROR! User error occurred, rollback failed!\nUser error: ${userError};\nRollback error: ${rollbackError}`
              ), { userError, rollbackError }))
            }
          } else {
            reject(Object.assign(new Error(
              `Transaction error, automatically rolled back: ${userError}`
            ), { userError }))
          }
        }
      })
    })
  }

  table(name, options) {
    return new Table(this, name, options)
  }

  async close() {
    await Promise.all((await this.pools).map(async pool => pool.close()))
  }
}
      
module.exports = Session