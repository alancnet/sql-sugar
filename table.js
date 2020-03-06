const { escapeField, objectCriteria } = require('./util')
const { raw, join } = require('nest-literal')

/**
 * Represents a Table in the schema.
 * @implements {Promise<Table>}
 */
class Table {
  constructor(session, name, options) {
    this.name = name
    this.options = options
    this.session = session
    this.type = null
    this.properties = new Map()
    this.jsonField = options.jsonField || 'json'
    this.idField = options.idField || 'id'
    this.jsonReadOnly = this.options.jsonReadOnly
    this._decodeObject = this._decodeObject.bind(this)
  }

  log(...args) {
    if (this.options.log) this.options.log(...args)
  }

  extends(table) {
    for ([name, opts] of table.properties.entries()) {
      this.property(name, opts)
    }
    return this
  }

  property(name, type, encode = v => v, decode = v => v) {
    if (this.properties.has(name)) throw new Error(`Property ${name} already defined.`)
    if (name === this.jsonField) throw new Error(`Don't specify the JSON field as a property.`)
    if (name === this.idField) throw new Error(`Don't specify the ID field as a property.`)
    this.properties.set(
      name,
      {
        type,
        encode: v => (v === null || v === undefined) ? null : encode(v),
        decode: v => (v === null || v === undefined) ? null : decode(v)
      }
    )
    return this
  }

  index() { /* noop */ return this }


  _getPresentFields(record) {
    return Array.from(this.properties.keys()).filter(key => record.hasOwnProperty(key))
  }
  _decodeObject(record) {
    const obj = {
      [this.idField]: null,
      ...JSON.parse(record[this.jsonField])
    }
    obj[this.idField] = record[this.idField]
    for (const [key, {decode}] of this.properties.entries()) {
      if (record.hasOwnProperty(key)) obj[key] = decode(record[key])
    }
    return obj
  }

  /**
   * Inserts a regular record.
   * @param {object} record Record data
   * @returns {object} The inserted record
   * @example await db.insert('User', {name: 'Tom Hanks'})
   * @returns {Record}
   */
  async insert(record) {
    const sql = this.session.sql
    const presentFields = this._getPresentFields(record)

    const fields = [
      ...presentFields,
      this.jsonField
    ]
    const values = [
      ...presentFields.map(field => this.properties.get(field).encode(record[field])),
      JSON.stringify(record)
    ]
    return this._decodeObject(await sql`
      insert into ${escapeField(this.name)}
      (${raw(fields.map(escapeField).join(', '))})
      output inserted.*
      values (${values})
    `.one())
  }

  /**
   * Gets a single record
   * @param {Reference} reference Record to get
   * @returns {Record}
   */
  async get(query) {
    const sql = this.session.sql
    if (typeof query === 'object') return (await sql`
      select * from ${escapeField(this.name)}
      where ${objectCriteria(query, this.properties)}
    `.recordset()).map(this._decodeObject)
    return this._decodeObject(await sql`
      select * from ${escapeField(this.name)} where ${escapeField(this.idField)} = ${query}
    `.one())
  }

  /** 
   * Updates records
   * @param {Reference} reference Records to update
   * @param {object} data Data to update
   */
  async update(record, data) {
    if (!data || !Object.values(data).filter(x => x !== undefined).length) throw new Error('Update requires changes')
    const sql = this.session.sql
    const fields = this._getPresentFields(record)
    const values = fields.map(field => this.properties.get(field).encode(record[field]))
    if (this.jsonReadOnly) {
      const extraFields = Object.keys(data).filter(field => !this.properties.has(field))
      if (extraFields.length) throw new Error(`Cannot update anonymous fields (${extraFields.join(', ')}) when jsonReadOnly is true.`)
    } else {
      fields.push(this.jsonField)
      values.push(JSON.stringify(record))
    }

    return this._decodeObject(await sql`
      update ${escapeField(this.name)}
      set ${fields.map((field, i) => sql`${escapeField(field)} = ${values[i]}`).reduce(join.with(', '))}
      output inserted.*
      where ${escapeField(this.idField)} = ${record[this.idField]}
    `.one())
  }

  /**
   * Deletes records
   * @param {(object|string|object[]|string[])} query Object query, record, record ID, array of records, or array of record IDs.
   */
  async delete(query) {
    const sql = this.session.sql
    if (typeof query === 'object') return await sql`
      delete from ${escapeField(this.name)}
      where ${objectCriteria(query, this.properties)}
    `
    return await sql`
      delete from ${escapeField(this.name)}
      where ${escapeField(this.idField)} = ${query}
    `
  }

  /**
   * Begins traversal with records
   * @param {Reference} reference 
   */
  traverse(reference) {
    const rids = toRidArray(reference)
    if (rids) {
      return new Traversal({ session: this.session, parent: null, expression: nest`select distinct(*) from [${rids.join(', ')}]`, chainable: false })
    } else {
      return new Traversal({ session: this.session, parent: null, expression: nest`select distinct(*) from ${escapeField(this.name)}`, chainable: false })
    }
  }
}

module.exports = Table