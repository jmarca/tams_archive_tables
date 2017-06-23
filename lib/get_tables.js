const cached_tables = new Map()
const fs = require('fs')


async function cache_tables(){
    return new Promise(function(resolve,reject){
        try {
            // create an array to stringify
            let serialized = JSON
                    .stringify(cached_tables, (k, v) => (v instanceof Map) ? [...v] : v)

            fs.writeFile('data/cached_tables.json'
                         ,serialized
                         ,'utf8'
                         ,(err, data) => {
                             if (err){
                                 // something went wrong
                                 return reject(err)
                             } else {
                                 // do nothing
                                 return resolve()
                             }
                             return null
                         })
            return null

        }
        catch (e) {
            // reject on throw
            console.log('error, ',e)
            return reject(e)
        }
        return null
    })
}

async function load_tables(){

    return new Promise(function(resolve,reject){
        try {

            fs.readFile('data/cached_tables.json', (err, data) => {
                if (err){
                    console.log(err)
                    if (err.code === 'ENOENT') {
                        console.error('data/cached_tables.json does not exist');
                        return resolve();
                    }
                    //throw err;
                    // do nothing
                } else {
                    // console.log('before')
                    // console.log(cached_tables)
                    const tempArray = JSON.parse(data)
                    // console.log(tempArray)
                    if(tempArray.length > 0){

                        tempArray.forEach( (v)=>{
                            //console.log(v)
                            let outer_key = v[0]
                            const keymap = new Map()
                            v[1].forEach( (vv)=>{
                                // console.log('vv is',vv)
                                const vvmap = new Map()
                                vv[1].forEach( (vvv)=>{
                                    // console.log('vvv is',vvv)
                                    let ii_key = vvv[0]
                                    let ii_val = vvv[1]
                                    vvmap.set(ii_key,ii_val)
                                })
                                let inner_key = vv[0]
                                keymap.set(inner_key,vvmap)
                            })
                            cached_tables.set(outer_key,keymap)
                        })
                    }
                    // console.log('after')
                    // console.log('done loading')
                    // console.log(JSON
                    //             .stringify(cached_tables, (k, v) => (v instanceof Map) ? [...v] : v)
                    //             )
                    //return reject('croak')
                }
                return resolve()
            })
        }
        catch (e) {
            // I really don't care if this throws.
            console.log('error, ',e)
            //return resolve()
        }
        return null
    })
}

/**
 * get_tables
 * @param {Object} config information for the query
 * @param {} client : psql client from node-pg library
 * @param {boolean} redo : truthy, redo the fetch.  falsy/null, cache is okay
 * @returns {}
 * @throws {}
 */
async function get_tables(config,client,redo){
    // here, we always possibly need to hit the db, so fail if no client is passed and redo is defined
    if(redo && (!client || client === undefined)){
        throw 'client required'
    }
    if(!redo && cached_tables.size === 0){
        try {
            console.log('try loading from fs')
            await load_tables()
        }catch (e){
            console.log('error, ',e)
            // if error, no harm done?
        }
    }
    const config_key = JSON.stringify(config.postgresql)
    if( !redo && cached_tables.has(config_key)){
        // console.log('do not redo')
        config.signaturearchives = new Map(
            Array.from(
                cached_tables.get(config_key)
            ))
        return config
    }
    const tables_query =
          "select schemaname,tablename from pg_tables where schemaname = 'archive' and tablename ~* '^signaturearchive'"
    const tresult = await client.query(tables_query)
    const trows = tresult.rows
    config.signaturearchives = new Map()
    const newmap = new Map()

    let q
    let qresult
    for (const r of trows) {

    //  return Promise.all(trows.map(async (r)=>{
        q = `select detstaid,
                   min(timestamp_full)::text as mintime,
                   max(timestamp_full)::text as maxtime
                   from archive.${r.tablename} group by detstaid;`
        console.log(q)
        qresult = await client.query(q)
        console.log('query done')

        qresult.rows.forEach( qr =>{

            if( newmap.has(qr.detstaid) ){
                newmap.set(qr.detstaid,
                           new Map(Array.from(newmap.get(qr.detstaid))
                                   .concat([[r.tablename,{'mintime':qr.mintime
                                                         ,'maxtime':qr.maxtime}]])))
            }else{
                newmap.set(qr.detstaid,
                           new Map([[r.tablename,{'mintime':qr.mintime
                                                  ,'maxtime':qr.maxtime}]]))
            }
        })
        //return newmap
    }
    cached_tables.set(config_key,newmap)
    config.signaturearchives = new Map(Array.from(newmap))
    await cache_tables()
    return config

}

/**
 * get_tables_for_detector
 *
 * This function is to get just the tables relevant to the passed in
 * detector.  It should be in the config.  also if you want a
 * particular time period pass that in with mintime, maxtime too.
 *
 * @param {Object} config : a configuration object with stuff
 * @param {Object} client : node-pg client connection
 */
async function get_tables_for_detector(config,client,redo){
    if(config.detstaid === undefined){
        throw 'config.detstaid required'
    }
    let local_config = Object.assign({},config)
    local_config = await get_tables(local_config,client,redo)
    config.signaturearchives = new Map()
    const sig_archives = local_config.signaturearchives.get(config.detstaid)
    let mintime,maxtime
    if(config.mintime){
        mintime = new Date(config.mintime)
    }
    if(config.maxtime){
        maxtime = new Date(config.maxtime)
    }
    const return_map = new Map()
    //if(mintime || maxtime){
        sig_archives.forEach( (details,tablename) =>{
            // check details to see if desired time falls inside database
            // table's times

            //            table 1         t3                  table 2
            //        min          max  a   b            min          max
            //             desiremin           desiremax

            if(
                (mintime && mintime > new Date(details.maxtime)) ||
                (maxtime && maxtime < new Date(details.mintime))
            ){
                // do not add this entry to return map
                //
                // either one of two cases here
                //
                // can't be in, because the maximum desired time is
                // less than the minimum time for this database table
                //
                // or it can't be in, because the minimum desired time
                // is more than the maximum time in the database table

                // console.log ('rejecting this case',
                //              mintime , new Date(details.maxtime),
                //              maxtime,new Date(details.mintime))

            }else{
                // console.log ('keeping this case',
                //              mintime , new Date(details.maxtime),
                //              maxtime,new Date(details.mintime))

                return_map.set(tablename,Object.assign({},details))
            }
        })

    //}
    config.signaturearchives = new Map([[config.detstaid,return_map]])
    return config
}

exports.get_tables = get_tables
exports.get_tables_for_detector = get_tables_for_detector
