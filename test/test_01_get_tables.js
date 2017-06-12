// make sure you can get the tables.

// jeez.  have to create the stuff and all that

const get_tables = require('../lib/get_tables').get_tables
const get_pool = require('psql_pooler').get_pool

const tap = require('tap')

const path    = require('path')
const rootdir = path.normalize(__dirname)

const config_file = rootdir+'/../test.config.json'
const config_okay = require('config_okay')

const utils =  require('./utils.js')
const exec_create_table =utils.exec_create_table
const drop = utils.drop_tables

const datafile_dir = process.cwd()+'/sql/'

const tablenames = []
const station_ids = [10001,
                     10002,
                     10003,
                     10004,
                     10005,
                     10006,
                     10007,
                     113,
                     3001,
                     4002,
                     46,
                     6001,
                     6002,
                     6003,
                     6004,
                     6005,
                     6007,
                     7001,
                     7002,
                     7003]

tap.plan(4)

tap.test('get_trip_request function exists',function (t) {
    t.plan(1)
    t.ok(get_tables,'insert trip request exists')
    t.end()
})
let pool


const test_query = async (_config,pool) => {

    let client = await pool.connect()

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


    await tap.test('run with client',async function(t){
        let config = Object.assign({},_config)
        t.plan(4)
        const task = await get_tables(config,client)
        // console.log(task)
        t.ok(task.signaturearchives,'there is a signaturearchives object')
        t.ok(task.signaturearchives.length=tablenames.length
             ,'got expected number of tables')
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
        t.same(tablenames,Map.keys(task.signaturearchives)
               ,'got expected tables list')
        t.end()
    })
    await client.release()
    return null


}

async function setup_dbs (config){

    let names = ['signaturearchive_1'
                 ,'signaturearchive_2' ]
    // ,'signaturearchive_3'
    // ,'signaturearchive_4'
    // ]
    return Promise.all(names.map( (name) =>{
        let filename = datafile_dir+name+'.sql'
        return exec_create_table(name,filename,config)
    })
                      )
        .then(listoftables=>{
            listoftables.forEach(t=>{ tablenames.push(t) })
            return listoftables
        })

}

config_okay(config_file)

    .then( async (config) => {
        // add the database name for pool
        config.postgresql.db = config.postgresql.signatures_db
        pool = await get_pool(config)

        try {
            const tables = await setup_dbs(config)
            // console.log('tables are ', tables)

            await test_query(config,pool)

            const client = await pool.connect()
            await drop(tables, client)
            await client.release()
        }catch(e){
            console.log('handling error',e)
        }finally{
            await pool.end()
            tap.end()
        }
    })
    .catch(async (err) =>{
        console.log('external catch statement triggered')
        console.log(err)
        await pool.end()
        await tap.end()
        throw new Error(err)

    })
