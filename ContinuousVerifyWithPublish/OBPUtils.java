/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class OBPUtils {

    private static OBPUtils instance;
    private final String rest_server_url;
    private final Integer rest_server_port;
    private final String channel_id;
    private final String chaincode_name;
    private static final Logger logger = Logger.getLogger(OBPUtils.class.getName());

    public static OBPUtils getInstance() {
        if (instance == null) {
            instance = new OBPUtils();
        }
        return instance;
    }

    /* Read OBP config parameters and build an instance */
    private OBPUtils() {
        InputStream inputStream = null;
        File configFile = new File("config.properties");
        Properties properties = new Properties();
        try {
            inputStream = new FileInputStream(configFile);
            properties.load(inputStream);
        } catch (FileNotFoundException ex) {
            throw new Error("Blockchain Platform Configuration File Absent!");
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
        }
        this.rest_server_url = properties.getProperty("rest_server_url");
        try {
            this.rest_server_port = Integer.parseInt(properties.getProperty("rest_server_port"));
        } catch (NumberFormatException ex) {
            throw new Error("REST Server Port should be an integer!");
        }
        this.channel_id = properties.getProperty("channel_id");
        this.chaincode_name = properties.getProperty("chaincode_name");
    }

    public String getRest_server_url() {
        return rest_server_url;
    }

    public Integer getRest_server_port() {
        return rest_server_port;
    }

    public String getChannel_id() {
        return channel_id;
    }

    public String getChaincode_name() {
        return chaincode_name;
    }

    public String getBlockchainPlatformURL(String operation) {
        return "https://" + getRest_server_url() + ":" + getRest_server_port()
                + "/restproxy/api/v2/channels/" + getChannel_id() + "/" + operation;
    }

    /* Read any errors from the OBP */
    public String errorReader(Reader streamReader) {
        String errorString = "";
        try {
            int charNum = streamReader.read();
            StringBuilder error = new StringBuilder();
            /* Read till the end of stream */
            while (charNum != -1) {
                char curr = (char) charNum;
                error.append(curr);
                charNum = streamReader.read();
            }
            try {
                /* Build Response */
                JSONObject errorResponse = new JSONObject(error.toString());
                errorString = errorResponse.getString("error");
            } catch (JSONException ex) {
                errorString = error.toString();
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return errorString;
    }

    /* Read any response from the OBP */
    public String readResponse(BufferedReader in) {
        StringBuilder response = new StringBuilder();
        try {
            String inputLine;
            /* Read till the end of Stream */
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return response.toString();
    }

    /* Build the JSON body for OBP request */
    public String JSONBodyBuilder(JSONArray argsArray, String mode) {
        /* Build Body for Request */
        JSONObject body = new JSONObject();
        if (mode.equals(Constants.OBP_POST)) {
            /* Get Txn Id and Nonce */
            String[] txnIdAndNonce = OBPConnection.getInstance().getTxnIdAndNonce();
            /* Txn ID */
            body.put("txid", txnIdAndNonce[0]);
            /* Nonce */
            body.put("nonce", txnIdAndNonce[1]);
            body.put("sync", true);
        }
        /* Chaincode to invoke */
        body.put("chaincode", chaincode_name);
        /* Chaincode args */
        body.put("args", argsArray);
        body.put("timeout", 18000);
        return body.toString();
    }

}
