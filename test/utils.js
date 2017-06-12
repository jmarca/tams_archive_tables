var exec = require('child_process').exec
async function create_table(tablename,file,client){
    const _tablename = 'signaturearchive_'+(Math.floor(10*Math.random()))+'_'+tablename
    const create_statement = `
    CREATE TABLE ${_tablename} (
       id integer NOT NULL,
       detstaid integer NOT NULL,
       dettype smallint,
       lane smallint NOT NULL,
       lane_dir smallint NOT NULL,
       "timestamp" integer,
       timestamp_sys integer,
       timestamp_full timestamp without time zone NOT NULL,
       samples smallint,
       vehicle_count integer,
       duration double precision,
       reserved smallint,
       psr double precision[],
       interpsig integer[],
       rawsig integer[],
       n_sample_count bigint[],
       PRIMARY KEY (id, detstaid, lane_dir, lane, timestamp_full)
    );`


    const create_response = await client.query(create_statement)
    const populate_statement = "\\\copy "+_tablename+" from '"+file+"';"
    console.log(populate_statement)
    const populate_response = await client.query(populate_statement)
    console.log(populate_response)
    return _tablename
}
exports.create_table = create_table


function exec_create_table(tablename,file,config){
    const psql_opts = config.postgresql
    const host = psql_opts.host
    const user = psql_opts.username
    // const pass = psql_opts.password
    const port = psql_opts.port
    const db   = psql_opts.db

    const _tablename = 'signaturearchive_'+(Math.floor(10*Math.random()))+'_'+tablename
    const create_statement = `\
    'CREATE TABLE ${_tablename} (  \
       id integer NOT NULL,\
       detstaid integer NOT NULL,\
       dettype smallint,\
       lane smallint NOT NULL,\
       lane_dir smallint NOT NULL,\
       "timestamp" integer,\
       timestamp_sys integer,\
       timestamp_full timestamp without time zone NOT NULL,\
       samples smallint,\
       vehicle_count integer,\
       duration double precision,\
       reserved smallint,\
       psr double precision[],\
       interpsig integer[],\
       rawsig integer[],\
       n_sample_count bigint[],\
       PRIMARY KEY (id, detstaid, lane_dir, lane, timestamp_full)\
    );'`

    const populate_statement = "'\\\copy "+_tablename+" from '"+file+"';'"
    // console.log(populate_statement)
    return new Promise(function (resolve, reject) {
        let commandline = ["/usr/bin/psql",
                           "-d", db,
                           "-U", user,
                           "-h", host,
                           "-p", port,
                           "-c", create_statement]
        // console.log(commandline)

        exec(commandline.join(' '),function(e,out,err){
            console.log('done create statement',out,err)
            if(e !== null){
                reject(e)
            }
            resolve(_tablename)
        })
        return null
    }).then(function(t){
        console.log('done creating with ',t)
        return new Promise(function(resolve,reject){
            let commandline = ["/usr/bin/psql",
                               "-d", db,
                               "-U", user,
                               "-h", host,
                               "-p", port,
                               "-c", populate_statement]
            // console.log(commandline)

            exec(commandline.join(' '),function(e,out,err){
                console.log('done populate statement')
                if(e !== null){
                    reject(e)
                }
                resolve(_tablename)
            })
            return null
        })
    }).catch( e =>{
        console.log('oops',e)
        throw e
    })

}

function drop_tables(tables,client){
    return Promise.all(tables.map(table =>{
        return client.query('drop table '+table+' cascade;')
    })
                      )
}
exports.exec_create_table = exec_create_table
exports.drop_tables = drop_tables
