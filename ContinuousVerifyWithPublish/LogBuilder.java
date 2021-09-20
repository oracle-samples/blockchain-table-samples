/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;


public class LogBuilder {

    private final String schema_name;
    private final String table_name;
    private final Integer instance_id;
    private final Integer chain_id;
    private final Integer seq_no;
    private final boolean result;
    private final String hash;
    private final String expected_hash;
    private static final Logger logger = Logger.getLogger(LogBuilder.class.getName());

    /* Hash Verification has failed */
    public LogBuilder(String schema_name, String table_name, Integer instance_id, Integer chain_id, Integer seq_no, String hash, String expected_hash) {
        this.schema_name = schema_name;
        this.table_name = table_name;
        this.instance_id = instance_id;
        this.chain_id = chain_id;
        this.seq_no = seq_no;
        this.result = false;
        this.hash = hash;
        this.expected_hash = expected_hash;
    }

    /* Hash Verification is successful */
    public LogBuilder(String schema_name, String table_name, Integer instance_id, Integer chain_id, Integer seq_no, String hash) {
        this.schema_name = schema_name;
        this.table_name = table_name;
        this.instance_id = instance_id;
        this.chain_id = chain_id;
        this.seq_no = seq_no;
        this.result = true;
        this.hash = hash;
        this.expected_hash = null;
    }

    public String getSchema_name() {
        return schema_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public Integer getInstance_id() {
        return instance_id;
    }

    public Integer getChain_id() {
        return chain_id;
    }

    public Integer getSeq_no() {
        return seq_no;
    }

    public boolean isResult() {
        return result;
    }

    public String getHash() {
        return hash;
    }

    public String getExpected_hash() {
        return expected_hash;
    }

    /* Publish the Log Object to a file or OBP */
    public void publish() {
        int mode = Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE();
        if (mode == Constants.MODE_LOCAL) {
            publishLocal();
        } else if (mode == Constants.MODE_OBP) {
            publishOBP();
        }
    }

    /* Publish the Log Object to a file */
    private void publishLocal() {
        String db_guid = DBUtils.getDBUtils().getDbGUID();
        String key = getSchema_name() + "_" + getTable_name() + "_" + db_guid + ".log";
        File logFile = new File(Utils.getUtils().cleanPath(key));
        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(logFile, true);
            String text;
            /* Check if verification failed or succeeded*/
            if (isResult()) {
                text = "Hash Verification successful for instance id : " + getInstance_id() + " , chain id : " + getChain_id()
                        + " , sequence no : " + getSeq_no() + ". Verified Hash: " + getHash() + "\n";
            } else {
                text = "Hash Verification Failed for instance id : " + getInstance_id() + " , chain id : " + getChain_id()
                        + " , sequence no : " + getSeq_no() + ". Expected Hash : " + getExpected_hash() + ". GOT : " + getHash() + "\n";
            }
            byte[] strToBytes = text.getBytes();
            outputStream.write(strToBytes);
        } catch (FileNotFoundException ex) {
            throw new Error("File not found!");
        } catch (IOException ex) {
            throw new Error("Unable to write");
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /* Publish log to OBP */
    private void publishOBP() {
        System.out.println("here");
        JSONArray args = new JSONArray();
        args.put("storeLog");
        args.put(getSchema_name());
        args.put(getTable_name());
        args.put(Utils.getUtils().getDbGUID());
        args.put(getInstance_id().toString());
        args.put(getChain_id().toString());
        args.put(getSeq_no().toString());
        args.put(String.valueOf(isResult()));
        args.put(getHash());
        if (!isResult()) {
            args.put(getExpected_hash());
        }
        String jsonBody = OBPUtils.getInstance().JSONBodyBuilder(args, Constants.OBP_POST);
        OBPConnection.getInstance().postData(jsonBody);
    }
}
