/* 
 * ContinousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


public class CheckPoint {

    private static CheckPoint instance;
    private JSONObject localObject;
    private static final Logger logger = Logger.getLogger(CheckPoint.class.getName());

    /* Get checkpoint instance */
    public static CheckPoint getInstance() {
        if (instance == null) {
            instance = new CheckPoint();
        }
        return instance;
    }

    /* Initialize checkpoint data to the point where we stopped */
    public void initCheckPoints(String schema, String table) {
        if (localObject == null) {
            int mode = Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE();
            if (mode == Constants.MODE_LOCAL) {
                initCheckPointsLocal(schema, table);
            } else if (mode == Constants.MODE_OBP) {
                initCheckPointsOBP(schema, table);
            }
        }
    }

    /* Make a savepoint */
    public void exportCheckPoints(String schema, String table) {
        int mode = Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE();
        if (mode == Constants.MODE_LOCAL) {
            exportCheckPointsLocal(schema, table);
        } else if (mode == Constants.MODE_OBP) {
            exportCheckPointsOBP(schema, table);
        }
    }

    /* Get the last seen sequence value for this chain_id */
    public int getSequenceValue(Integer instance_id, Integer chain_id) {
        int CONTINUOUS_VERIFICATION_MODE = Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE();
        if (CONTINUOUS_VERIFICATION_MODE == Constants.MODE_OFF) {
            return 0;
        }
        /* If the local object doesn't know this instance. We create a new array */
        if (!localObject.has(instance_id.toString())) {
            JSONArray jSONArray = new JSONArray(new int[32]);
            localObject.put(instance_id.toString(), jSONArray);
        }
        return (int) localObject.getJSONArray(instance_id.toString()).get(chain_id);
    }

    /* Set the last seen sequence value for this chain_id */
    public void setLastSeenSequence(Integer instance_id, Integer chain_id, Integer sequence_no) {
        int CONTINUOUS_VERIFICATION_MODE = Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE();
        if (CONTINUOUS_VERIFICATION_MODE == Constants.MODE_OFF) {
            return;
        }
        localObject.getJSONArray(instance_id.toString()).put(chain_id.intValue(), sequence_no);
    }

    /* Export checkpoints in a local file */
    private void exportCheckPointsLocal(String schema, String table) {
        FileWriter fileWriter = null;
        try {
            String db_guid = DBUtils.getDBUtils().getDbGUID();
            String file = schema + "_" + table + "_" + db_guid + ".json";
            fileWriter = new FileWriter(Utils.getUtils().cleanPath(file));
            fileWriter.write(localObject.toString());
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
    }

    /* Export Checkpoints to OBP */
    private void exportCheckPointsOBP(String schema, String table) {
        /* Build the arguments array */
        JSONArray args = new JSONArray();
        args.put("writeMetadata");
        args.put(schema);
        args.put(table);
        args.put(DBUtils.getDBUtils().getDbGUID());
        args.put(localObject.toString());
        /* Get the JSON Body to send as a part of the request */
        String jsonBody = OBPUtils.getInstance().JSONBodyBuilder(args, Constants.OBP_POST);
        OBPConnection.getInstance().postData(jsonBody);
    }

    /* Initialize checkpoints from a local file */
    private void initCheckPointsLocal(String schema, String table) {
        InputStream is = null;
        try {
            String db_guid = DBUtils.getDBUtils().getDbGUID();
            String file = schema + "_" + table + "_" + db_guid + ".json";
            File f = new File(Utils.getUtils().cleanPath(file));
            if (!f.exists()) {
                localObject = new JSONObject();
            } else {
                is = new FileInputStream(f);
                JSONTokener jSONTokener = new JSONTokener(is);
                localObject = new JSONObject(jSONTokener);
            }
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
    }

    /* Initialize checkpoints from OBP */
    private void initCheckPointsOBP(String schema, String table) {
        JSONArray args = new JSONArray();
        args.put("readMetadata");
        args.put(schema);
        args.put(table);
        args.put(DBUtils.getDBUtils().getDbGUID());
        String jsonBody = OBPUtils.getInstance().JSONBodyBuilder(args, Constants.OBP_GET);
        localObject = OBPConnection.getInstance().fetchData(jsonBody);
    }
}
