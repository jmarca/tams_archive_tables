// make sure you can get the tables.

// jeez.  have to create the stuff and all that

const get_tables = require('../lib/get_tables')
const get_pool = require('loopshare_companies').get_pool

const tap = require('tap')

const path    = require('path')
const rootdir = path.normalize(__dirname)

const config_file = rootdir+'/../test.config.json'
const config_okay = require('config_okay')

tap.plan(4)

tap.test('get_trip_request function exists',function (t) {
    t.plan(1)
    t.ok(get_tables,'insert trip request exists')
    t.end()
})
let pool


const test_query = async (_config) => {

    let client

    await tap.test('run query without client',async function(t) {
        let config = Object.assign({},_config)
        t.plan(1)
        try{
            await get_tables(config)
            t.fail('should not succeed without client')
            t.end()
        }catch (query_error){
            t.match(query_error,/client required/,'should fail query without client passed in')
            t.end()
        }
    })

    client = await pool.connect()
    await client.query('BEGIN')
    await tap.test('run with client',async function(t){
        let config = Object.assign({},_config)
        t.plan(4)
        const task = await get_tables(config,client)
        // console.log(task)
        t.ok(task.signaturearchives,'there is a signaturearchives object')
        t.ok(task.signaturearchives.length=4,'got expected number of tables')
        task.signaturearchives.forEach( table => {

            t.ok(table.size>0,'got a non-empty map')

            table.forEach((value,key)=>{
                t.ok(station_ids.indexOf(key)>-1,'got a meaningful key')
                t.same(['max_time','min_time','tablename'],Map.keys(value)
                       ,'map has expected entries')
                return null
            })

            return null
        })
        t.same(fake_table_names,Map.keys(task.signaturearchives)
               ,'got expected tables list')
        t.end()
    })

    return null


}


config_okay(config_file)

    .then( async (config) => {
        // add the database name for pool
        config.postgresql.db = config.postgresql.loopshare_db
        pool = await get_pool(config)

        await setup_dbs(config)

        await test_query(config)
        await pool.end()
        tap.end()
    })
    .catch( (err) =>{
        console.log('external catch statement triggered')
        console.log(err)
        tap.end()
        throw new Error(err)

    })
