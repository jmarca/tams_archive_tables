const mylib = require('.')
const get_tables = mylib.get_tables
const get_pool = require('psql_pooler').get_pool

const rw = require("rw").dash
const commander = require("commander")

const path    = require('path')
const rootdir = path.normalize(__dirname)


commander
    .version(require("./package.json").version)
    .usage("[options] [file]")
    .option("--config <file>","configuration file to use; defaults to config.json")
    .option("--directory <dir>","output directory for files, defaults to data","data")
    .parse(process.argv);


const config_file = rootdir+'/'+commander.config
const config_okay = require('config_okay')
const output_path = rootdir+'/'+commander.directory


config_okay(config_file)

    .then( async (config) => {
        // add the database name for pool
        config.postgresql.db = config.postgresql.signatures_db
        // console.log(config)
        let pool
        let client
        try {
            pool = await get_pool(config)
            client = await pool.connect()
            const task = await get_tables(config,client)
            console.log('done')
        }catch(e){
            console.log('handling error',e)
        }finally{
            await client.release()
            await pool.end()
        }
        return null
    })
    .catch( (err) =>{
        console.log('external catch statement triggered')
        console.log(err)
        throw new Error(err)
    })
