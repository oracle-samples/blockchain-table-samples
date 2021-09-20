'use strict';
const shim = require('fabric-shim');
const util = require('util');
const crypto = require('crypto');

var Chaincode = class {
    /* Initialize the chaincode */
    async Init(stub) {
        console.info('========= Initialize Chaincode =========');
        let ret = stub.getFunctionAndParameters();
        console.info(ret);
        return shim.success();
    }

    /* Invoke the appropriate chaincode method */
    async Invoke(stub) {
        console.info('Transaction ID: ' + stub.getTxID());
        let ret = stub.getFunctionAndParameters();
        console.info(ret);
        let method = this[ret.fcn];
        if (!method) {
            console.error('no method of name:' + ret.fcn + ' found');
            throw new Error('Received unknown function ' + ret.fcn + ' invocation');
        }

        console.info('\nCalling method : ' + ret.fcn);
        try {
            let payload = await method(stub, ret.params, this);
            return shim.success(payload);
        } catch (err) {
            console.log(err);
            return shim.error(err);
        }
    }

    /* Store the Information about the verified row.  This function is 
       responsible for building the log history for the verified blockchain
       table and also adds the log entry to the appropriate buckets and
       queues.
    */
    async storeLog(stub, args, thisClass) {
        let jsonResp = {};
        let log = {};
        if (args.length < 8 && args.length > 9) {
            throw new Error('Incorrect number of arguments. Expecting 8 or 9. Expected arguments : <SCHEMA> <TABLE_NAME> <PDB_GUID> <INSTANCE_ID> <CHAIN_ID> <SEQUENCE_NO> <VERIFICATION_RESULT> <VERIFIED_HASH> <FAILED_HASH - IN CASE OF VERIFICATION FAILURE>');
        }
        /* Get all input elements */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        let inst_id = args[3];
        let chain_id = args[4];
        let seq_no = args[5];
        let verification_result = args[6];
        let got_hash = args[7];

        if (got_hash.length <= 0) {
            throw new Error('hash must be a non-empty string');
        }
        let sequence_no = parseInt(seq_no);
        if(isNaN(sequence_no) || !Number.isInteger(parseFloat(seq_no))){
            throw new Error('sequence no must be a numeric string. GOT : ' + seq_no);        
        }
        /* Get the key for this bucket */
        let id = getId(schema , table_name , pdb_guid , inst_id , chain_id, sequence_no);
        let res = await stub.getState(id);
        /* If the array doesn't exist, we create a array with bucket size of 100 */
        if(!res.toString()) {
            res = new Array(100).fill();
        }else {
            res = JSON.parse(res.toString('utf8'));
        }
        /*Build the log */
        log.instance_id = parseInt(inst_id);
        log.chain_id = parseInt(chain_id);
        log.sequence_no = sequence_no;
        log.result = JSON.parse(verification_result);
        log.got_hash = got_hash;
        if(!log.result) {
            let exp_hash = args[8];
            if (exp_hash.length <= 0) {
                throw new Error('hash must be a non-empty string');
            }
            log.expected_hash = exp_hash;
            /* Record the failure in the failed queue */
            let failedQueueKey = getOperationKey(schema, table_name, pdb_guid, 'failedQueue');
            let failedQueue = await stub.getState(failedQueueKey);
            if(!failedQueue.toString()) {
                failedQueue = []
            }else {
                failedQueue = JSON.parse(failedQueue.toString('utf8'));
            }
            failedQueue.push(log);
            await stub.putState(failedQueueKey, Buffer.from(JSON.stringify(failedQueue),'utf8'));
        }
        /* Calculate bucket index */
        let idx = (sequence_no - 1) % 100;
        res[idx] = log;
        /* Get the key for last 100 array */
        let last100Key = getOperationKey(schema, table_name, pdb_guid, 'last100');
        let last100 = await stub.getState(last100Key);
        /* If this is the first record create a new array, else append */
        if(!last100.toString()) {
            last100 = [];
        }else {
            last100 = JSON.parse(last100.toString('utf8'));
        }
        /* Get rid of the oldest record if necessary and push new record */
        if(last100.length > 100) {
            last100.shift();
        }
        last100.push(log);
        await stub.putState(last100Key, Buffer.from(JSON.stringify(last100),'utf8'));
        await stub.putState(id, Buffer.from(JSON.stringify(res),'utf8'));
    }

    /* Read the information about a previously verified row. This function
       fetches a log record specific to a blockchain table row identified by
       (instance_id, chain_id, sequence_no).
    */
    async readLog(stub, args, thisClass) {
        let jsonResp = {};
        if (args.length != 6) {
            throw new Error('Incorrect number of arguments. Expecting 6. Expected arguments: <SCHEMA> <TABLE_NAME> <PDB_GUID> <INSTANCE_ID> <CHAIN_ID> <SEQUENCE_NO>');
        }
        /* Fetch input arguments */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        let inst_id = args[3];
        let chain_id = args[4];
        let seq_no = args[5];
        let sequence_no = parseInt(seq_no);
        if(isNaN(sequence_no) || !Number.isInteger(parseFloat(seq_no))){
            throw new Error('sequence no must be a numeric string. GOT : ' + seq_no);        
        }
        const id = getId(schema, table_name, pdb_guid, inst_id, chain_id, sequence_no);
        let result = await stub.getState(id);
        if(!result.toString()) {
            jsonResp.Error = 'Log not found for this schema , table , inst_id , chain_id and seq_no';
            throw new Error(JSON.stringify(jsonResp));
        }
        let idx = (sequence_no - 1) % 100;
        result = JSON.parse(result.toString('utf8'));
        if(result[idx] === null) {
           jsonResp.Error = 'Log not found for this schema , table , inst_id , chain_id and seq_no';
           throw new Error(JSON.stringify(jsonResp));
        }
        return Buffer.from(JSON.stringify(result[idx], 'utf8'));
    }

    /* Fetch the logs for a specific chain_id. This function is responsible
       for fetching the last N records for a specific chain_id of the
       specified blockchain table */
    async readChainLogs(stub, args, thisClass) {
        if(args.length < 5 || args.length > 6) {
            throw new Error('Incorrect number of arguments. Expecting 5-6. Expected Arguments: <SCHEMA> <TABLE_NAME> <PDB_GUID> <INSTANCE_ID> <CHAIN_ID> <LIMIT - OPTIONAL>');
        }
        /* Fetch input arguments */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        let instance_id = parseInt(args[3]);
        if(isNaN(instance_id) || !Number.isInteger(parseFloat(instance_id))){
            throw new Error('instance id must be a numeric string. GOT: ' + instance_id);        
        }
        let chain_id = parseInt(args[4]);
        if(isNaN(chain_id) || !Number.isInteger(parseFloat(chain_id))){
            throw new Error('chain id must be a numeric string. GOT: ' + chain_id);        
        }
        let limit = -1;
        if(args.length == 6) {
            limit = parseInt(args[5]);
            if(isNaN(limit) || !Number.isInteger(parseFloat(limit))){
                throw new Error('limit must be a numeric string. GOT : ' + limit);        
            }
        }
        /* fetch the last stored value for sequence number for this chain */
        const metaDataKey = getOperationKey(schema, table_name, pdb_guid, 'metadata');
        let metaData = await stub.getState(metaDataKey);
        if(!metaData.toString()) {
            throw new Error('Chain Metadata not found!');
        }
        metaData = JSON.parse(metaData.toString('utf8'));
        let last_seq = metaData[instance_id.toString()][chain_id - 1];
        if(limit > last_seq) {
            throw new Error('Limit Cannot be greater than last known sequence number! Last Sequence number : ' + last_seq);
        }
        limit = limit >= 0 ? limit : last_seq;
        let result = [];
        while(limit > 0) {
            let bucket_key = getId(schema, table_name, pdb_guid, instance_id , chain_id , last_seq);
            /* number of elements in current bucket */
            let elements_in_bucket = ((last_seq - 1) % 100) + 1;
            /* get the current bucket */
            let curr_bucket = await stub.getState(bucket_key);
            if(!curr_bucket.toString()) {
                throw new Error('Logs does not exist! Try again.');
            }else {
                curr_bucket = JSON.parse(curr_bucket.toString('utf8'));
            }
            /* If the number of items remaining is less than available items we need last of the few */
            if(limit <= elements_in_bucket) {
                result = (curr_bucket.slice(elements_in_bucket - limit , elements_in_bucket)).concat(result);
                limit = 0;
            /* Number of items needed is more than number of elements in current bucket */
            } else {
                result = (curr_bucket.slice(0,elements_in_bucket)).concat(result);
                last_seq = last_seq - elements_in_bucket;
                limit = limit - elements_in_bucket;
            }
        }
        return Buffer.from(JSON.stringify(result), 'utf8');
    }

    /* Read the metadata about the last verified sequence numbers for a
       blockchain table. This function is responsible for returning 
       information about the number of rows verified per instance,
       per chain for a specified blockchain table.
    */
    async readMetadata(stub, args, thisClass) {
        if(args.length != 3) {
            throw new Error('Incorrect number of arguments. Expecting 3. Expecting: <SCHEMA> <TABLE_NAME> <PDB_GUID>');
        }
        /* Fetch input arguments */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        /* get operation key */
        const k = getOperationKey(schema, table_name, pdb_guid, 'metadata');
        let result = await stub.getState(k);
        return result;
    }

    /* Write the metadata about the last verified sequence numbers for a 
       blockchain table. This function is responsible for writing 
       information about the last seen sequence number per instance, per
       chain for a specified blockchain table. This is needed so that
       we can continue our verification from the last seen checkpoint 
       when CONTINUOUS_VERIFICATION_MODE is active.
    */
    async writeMetadata(stub, args, thisClass) {
        if(args.length != 4) {
            throw new Error('Incorrect number of arguments. Expecting 4. Expecting: <SCHEMA> <TABLE_NAME> <PDB_GUID> <METADATA_OBJECT>');
        }
        /* Fetch input arguments */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        /* get operation key */
        const k = getOperationKey(schema, table_name, pdb_guid, 'metadata');
        await stub.putState(k, Buffer.from(args[3]));
    }

    /* Fetches the last 100 rows seen globally irrespective of the table
       owner, name , instance_id, chain_id or sequence number.
    */
    async fetchLast100(stub, args, thisClass) {
        if(args.length != 3) {
            throw new Error('Incorrect number of arguments. Expecting 3. Expecting: <SCHEMA> <TABLE_NAME> <PDB_GUID>');
        }
        /* Fetch input arguments */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        /* get operation key */
        let last100Key = getOperationKey(schema, table_name, pdb_guid, 'last100');
        let result = await stub.getState(last100Key);
        return result;
    }

    /* Fetch all / last N rows which failed verification */
    async getFailedRows(stub, args, thisClass) {
        if(args.length < 3 || args.length > 4) {
            throw new Error('Incorrect number of arguments. Expecting 3-4. Expecting : <SCHEMA> <TABLE_NAME> <PDB_GUID> <LIMIT - OPTIONAL>');
        }
        /* Fetch input arguments */
        let schema = args[0];
        let table_name = args[1];
        let pdb_guid = args[2];
        /* get operation key */
        let failedQueueKey = getOperationKey(schema, table_name, pdb_guid, 'failedQueue');
        let result = await stub.getState(failedQueueKey);
        result = JSON.parse(result.toString('utf8'));
        if(args.length == 4) {
            let limit = parseInt(args[3]);
            if(isNaN(limit) || !Number.isInteger(parseFloat(limit))){
                throw new Error('limit must be a numeric string. GOT : ' + limit);        
            }
            if(limit > result.length) {
                limit = result.length;
            }
            result = result.splice(limit * - 1);
        }
        return Buffer.from(JSON.stringify(result), 'utf8');
    }
};

/* Get the key for the appropriate bucket in case we are appending a row log */
function getId(schema , table , pdb_guid , instance_id , chain_id , seq_no) {
    let rowHash = {};
    if (schema.length <= 0) {
        throw new Error('schema name must be a non-empty string');
    }
    if (table.length <= 0) {
        throw new Error('table name must be a non-empty string');
    }
    if(pdb_guid.length <= 0) {
        throw new Error('pdb guid cannot be non-empty');
    }
    rowHash.schema = schema;
    rowHash.table = table;
    rowHash.pdb_guid = pdb_guid;
    rowHash.instance_id = parseInt(instance_id);
    if(isNaN(rowHash.instance_id) || !Number.isInteger(parseFloat(rowHash.instance_id))){
        throw new Error('instance id must be a numeric string. GOT: ' + instance_id);        
    }
    rowHash.chain_id = parseInt(chain_id);
    if(isNaN(rowHash.chain_id) || !Number.isInteger(parseFloat(rowHash.chain_id))){
        throw new Error('chain id must be a numeric string. GOT: ' + chain_id);        
    }
    rowHash.sequence_bucket = Math.floor((seq_no - 1) / 100);
    const id = crypto.createHash('sha256').update(JSON.stringify(rowHash)).digest('hex');
    return id;
}

/* Get the key for the appropriate queue in case we are doing a special 
   operation like fetchlast100, getFailedRows etc.
*/
function getOperationKey(schema, table, pdb_guid, operation) {
    let key = {};
    key.schema = schema;
    key.table = table;
    key.pdb_guid = pdb_guid;
    key.type = operation;
    return JSON.stringify(key);
}

shim.start(new Chaincode());