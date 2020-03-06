const { nest, raw, join } = require('nest-literal')
const isEqual = require('lodash/isEqual')

const escapeField = field => raw(`[${field}]`)

const sqlify = value => raw(
  typeof value === 'string' ? `N'${value.split("'").join("''")}'`
  : typeof value === 'number' ? value.toString()
  : Array.isArray(value) ? value.map(sqlify).join(', ')
  : null
)

const sqlifyTemplate = (template) => {
  return nest(template.callSite, ...template.substitutions.map(sqlify)).toString()
}

const trimParameters = (template) => {
  const { callSite, substitutions } = template
  const maxParams = 2100
  for (let i = maxParams; i < substitutions.length; i++) {
    substitutions[i] = sqlify(substitutions[i])
  }
  return nest(callSite, ...substitutions)
}

const parens = (arr, delim) => {
  if (arr.length === 1) return arr[0]
  return nest`(${arr.reduce(join.with(delim))})`
}
const ops = {
  $eq: (f, v) => nest`[${raw(f)}] = ${v}`,
  $ne: (f, v) => nest`[${raw(f)}] != ${v}`,
  $gt: (f, v) => nest`[${raw(f)}] > ${v}`,
  $gte: (f, v) => nest`[${raw(f)}] >= ${v}`,
  $lt: (f, v) => nest`[${raw(f)}] < ${v}`,
  $lte: (f, v) => nest`[${raw(f)}] <= ${v}`,
  $in: (f, v) => nest`[${raw(f)}] in (${v})`,
  $nin: (f, v) => nest`[${raw(f)}] not in (${v})`
}

const objectCriteria = (obj, parameters = new Map()) => {
  const groups = {
    $and: (v) => parens(v.map(v => objectCriteria(v, parameters)), ' AND '),
    $or: (v) => parens(v.map(v => objectCriteria(v, parameters)), ' OR ')
  }
  const encode = (field, value) => parameters.has(field) ? parameters.get(field).encode(value) : value
  if (Object.keys(obj).length === 0) return raw('1 = 1')
  if (Object.keys(obj).some(x => x.startsWith('$'))) {
    return parens(Object.entries(obj).map(([key, value]) => {
      if (!key.startsWith('$')) throw new Error('Cannot mix directives with fields')
      return groups[key](value)
    }), ' AND ')
  } else {
    return parens(Object.entries(obj).map(([field, value]) => {
      if (value && Object.keys(value).some(x => x.startsWith('$'))) {
        return parens(Object.entries(value).map(([key, value]) => {
          if (!key.startsWith('$')) throw new Error('Cannot mix directives with fields')
          return ops[key](field, encode(field, value))
        }), ' AND ')
      } else {
        return ops.$eq(field, encode(field, value))
      }
    }), ' AND ')
  }
}

const whatChanged = (record, data) => {
  const changed = Object.entries(data).filter(([key, value]) => !isEqual(value, record[key]))
  if (changed.length) return Object.fromEntries(changed)
  else return null
}


module.exports = {
  sqlify, sqlifyTemplate,
  trimParameters, objectCriteria,
  escapeField, whatChanged
}