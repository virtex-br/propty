const Pool = require('pg').Pool
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'trader',
  password: 'postgres',
  port: 5432,
})

const getPostcode = (request, response) => {
  const street = request.params.street
  const town = request.params.town
  console.log(request.params)
  pool.query("select distinct postcode from addresses where street = upper($1) and town = upper($2)", [street, town], (error, results) => {
    if (error) {
      throw error
    }
    response.status(200).json(results.rows)
  })
}

const query = (request, response, query) => {
  const postcode = request.params.postcode
  const radius = request.params.radius
  console.log(request.params)
  pool.query("SELECT * FROM price_by_area(upper($1), $2)", [postcode, radius], (error, results) => {
    if (error) {
      throw error
    }
    response.status(200).json(results.rows)
  })
}

module.exports = {query, getPostcode}
