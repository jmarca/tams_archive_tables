/**
 * get_tables
 * @param {Object} config information for the query
 * @param {} client : psql client from node-pg library
 * @returns {}
 * @throws {}
 */
async function get_tables(config,client){
    if(!client || client === undefined){
        throw 'client required'
    }

    const tables_query =
          "select schemaname,tablename from pg_tables where schemaname = 'archive' and tablename ~* '^signaturearchive'"
    const tresult = await client.query(tables_query)
    const trows = tresult.rows
    config.signaturearchives = new Map()
    return Promise.all(trows.map(async (r)=>{
        const q = `select detstaid,
                   min(timestamp_full)::text as mintime,
                   max(timestamp_full)::text as maxtime
                   from archive.${r.tablename} group by detstaid;`
        const qresult = await client.query(q)
        qresult.rows.forEach( qr =>{

            if( config.signaturearchives.has(qr.detstaid) ){
                let mymap = config.signaturearchives.get(qr.detstaid)
                // console.log(qr)
                // console.log(mymap)
                mymap.set(r.tablename,{'mintime':qr.mintime
                                       ,'maxtime':qr.maxtime})
            }else{
                let detectormap = new Map([[r.tablename,{'mintime':qr.mintime
                                                    ,'maxtime':qr.maxtime}]])
                config.signaturearchives.set(qr.detstaid,detectormap)
            }
            //console.log(config.signaturearchives)
            return null
        })
    }))
        .then(result =>{
            return config
        })
}

exports.get_tables = get_tables
