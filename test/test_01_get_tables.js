// make sure you can get the tables.

// jeez.  have to create the stuff and all that

const get_tables_lib = require('../.')
const get_tables = get_tables_lib.get_tables
const get_tables_for_detector = get_tables_lib.get_tables_for_detector

const get_pool = require('psql_pooler').get_pool

const tap = require('tap')

const path    = require('path')
const rootdir = path.normalize(__dirname)

const config_file = rootdir+'/../test.config.json'
const config_okay = require('config_okay')

const utils =  require('./utils.js')
const exec_create_table =utils.exec_create_table
const drop = utils.drop_tables
const create_schema = utils.create_schema

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

tap.test('get_tables function exists',function (t) {
    t.plan(1)
    t.ok(get_tables,'get_tables exists')
    t.end()
})
let pool


const test_query = async (_config,pool) => {


    await tap.test('run query without client',async function(t) {
        let config = Object.assign({},_config)
        t.plan(1)
        try{
            await get_tables(config,null,1)
            t.fail('should not succeed without client')
            t.end()
        }catch (query_error){
            t.match(query_error,/client required/,'should fail query without client passed in')
            t.end()
        }
    })

    let client = await pool.connect()
    await client.query("BEGIN;")
    try {
        let counter = 0
        await tap.test('run with client',async function(t){
            let config = Object.assign({},_config)
            let planned_tests = 4+4*(station_ids.length)+1

            t.plan(planned_tests)
            const task = await get_tables(config,client)
            // console.log('task result',task.signaturearchives)
            t.ok(task.signaturearchives,'there is a signaturearchives object')
            t.ok(task.signaturearchives.size = station_ids.length
                 ,'got expected number of map entries')

            const archive_map = task.signaturearchives
            // console.log(archive_map.size)
            archive_map.forEach( (table_entry,detectorid) => {
                // console.log(detectorid,table_entry,counter++,planned_tests)
                // planned_tests -= 2

                t.ok(station_ids.indexOf(detectorid)>-1,'got a meaningful key')
                t.ok(table_entry.size>0,'got a non-empty map')
                if(detectorid === 6002){
                    t.is(table_entry.size,2,'should have two db tables for detector 6002')
                }
                table_entry.forEach((value,key)=>{
                   // console.log(detectorid,table_entry,counter++,planned_tests)
                   //  planned_tests -= 2

                    let value_keys = Object.keys(value )

                    t.same(['mintime','maxtime'],value_keys
                           ,'map has expected entries')
                    t.ok(tablenames.indexOf(key)>-1,'got a meaningful key')
                    return null
                })

                return null
            })

            t.end()
        })
    }catch (e){
        throw (e)
    }finally{
        await client.query("ROLLBACK;")
        await client.release()
    }


    // testing get_tables_for_detector

    client = await pool.connect()
    await client.query("BEGIN;")
    try {
        let counter = 0
        await tap.test('run with client, one detector',async function(t){
            let config = Object.assign({},_config)
            let planned_tests = 2 + 9 + 7 + 7 + 3
            t.plan(planned_tests)

            // first, without detstaid, shoudl throw
            try {
                await get_tables_for_detector(config,client)
                t.fail('should have thrown')
            }catch (e){
                t.pass('threw okay')
                t.ok(/detstaid required/.test(e),'got expected error message')
            }

            config.detstaid = 6002
            let task = await get_tables_for_detector(config,client)
            // console.log('task result',task.signaturearchives)
            t.ok(task.signaturearchives,'there is a signaturearchives object')
            t.ok(task.signaturearchives.size = 1
                 ,'got expected number of map entries')

            let archive_map = task.signaturearchives
            // console.log(archive_map.size)
            archive_map.forEach( (table_entry,detectorid) => {
                // console.log(detectorid,table_entry,counter++,planned_tests)
                // planned_tests -= 2

                t.ok(station_ids.indexOf(detectorid)>-1,'got a meaningful key')
                t.ok(table_entry.size>0,'got a non-empty map')
                t.is(table_entry.size,2,'should have two db tables for detector 6002')

                table_entry.forEach((value,key)=>{
                   // console.log(detectorid,table_entry,counter++,planned_tests)
                   //  planned_tests -= 2

                    let value_keys = Object.keys(value )

                    t.same(['mintime','maxtime'],value_keys
                           ,'map has expected entries')
                    t.ok(tablenames.indexOf(key)>-1,'got a meaningful key')
                    return null
                })

                return null
            })

            // now set a minimum time
            config = Object.assign({},_config)
            config.detstaid = 6002
            config.mintime = '2015-12-31 20:00:16.001'
            //config.maxtime = '2015-12-31 20:00:16.001'

            task = await get_tables_for_detector(config,client)
            // console.log('task result',task.signaturearchives)
            t.ok(task.signaturearchives,'there is a signaturearchives object')
            t.ok(task.signaturearchives.size = 1
                 ,'got expected number of map entries')

            archive_map = task.signaturearchives
            // console.log(archive_map.size)
            archive_map.forEach( (table_entry,detectorid) => {
                // console.log(detectorid,table_entry,counter++,planned_tests)
                // planned_tests -= 2

                t.ok(station_ids.indexOf(detectorid)>-1,'got a meaningful key')
                t.ok(table_entry.size>0,'got a non-empty map')
                t.is(table_entry.size,1,'now should have one db tables for detector 6002')

                table_entry.forEach((value,key)=>{
                   // console.log(detectorid,table_entry,counter++,planned_tests)
                   //  planned_tests -= 2

                    let value_keys = Object.keys(value )

                    t.same(['mintime','maxtime'],value_keys
                           ,'map has expected entries')
                    t.ok(tablenames.indexOf(key)>-1,'got a meaningful key')
                    return null
                })

                return null
            })

            // now set a maximum time
            config = Object.assign({},_config)
            config.detstaid = 6002
            config.maxtime = '2016-01-01 00:00:00.001'

            task = await get_tables_for_detector(config,client)
            // console.log('task result',task.signaturearchives)
            t.ok(task.signaturearchives,'there is a signaturearchives object')
            t.ok(task.signaturearchives.size = 1
                 ,'got expected number of map entries')

            archive_map = task.signaturearchives
            // console.log(archive_map.size)
            archive_map.forEach( (table_entry,detectorid) => {
                // console.log(detectorid,table_entry,counter++,planned_tests)
                // planned_tests -= 2

                t.ok(station_ids.indexOf(detectorid)>-1,'got a meaningful key')
                t.ok(table_entry.size>0,'got a non-empty map')
                t.is(table_entry.size,1,'now should have one db tables for detector 6002')

                table_entry.forEach((value,key)=>{
                   // console.log(detectorid,table_entry,counter++,planned_tests)
                   //  planned_tests -= 2

                    let value_keys = Object.keys(value )

                    t.same(['mintime','maxtime'],value_keys
                           ,'map has expected entries')
                    t.ok(tablenames.indexOf(key)>-1,'got a meaningful key')
                    return null
                })

                return null
            })

            // now set a min and max time that isn't covered
            config = Object.assign({},_config)
            config.detstaid = 6002
            config.mintime = '2016-01-02 00:00:00.001'
            config.maxtime = '2016-01-02 00:01:00.001'

            task = await get_tables_for_detector(config,client,true)
            t.ok(task.signaturearchives,'there is a signaturearchives object')
            t.ok(task.signaturearchives.size = 1
                 ,'got a non-empty map')
            t.is( task.signaturearchives.get(6002).size, 0
                 ,'but nothing in it')


            t.end()
        })
    }catch (e){
        throw (e)
    }finally{
        await client.query("ROLLBACK;")
        await client.release()
    }


    return null

}

async function setup_dbs (config){

    let names = ['signaturearchive_1'
                 ,'signaturearchive_2'
                 ,'signaturearchive_3'
                 ,'signaturearchive_4'
                ]
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
        try {
            pool = await get_pool(config)
            let  client = await pool.connect()
            try{
                await drop(client)
                await create_schema(client)
            }catch(e){
                console.log('problem with drop create, ',e)
            }finally {
                await client.release()
            }
            const tables = await setup_dbs(config)
            // console.log('tables are ', tables)

            await test_query(config,pool)

            client = await pool.connect()
            try{
                await drop(client)
            }catch(e){
                throw e
            }finally{
                client.release()
            }

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
        throw new Error(err)
    })
