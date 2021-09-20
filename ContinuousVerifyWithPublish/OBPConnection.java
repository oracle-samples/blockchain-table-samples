/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;
import org.json.JSONObject;


public class OBPConnection {

    private static OBPConnection instance;
    private static final Logger logger = Logger.getLogger(OBPConnection.class.getName());

    public static OBPConnection getInstance() {
        if (instance == null) {
            instance = new OBPConnection();
        }
        return instance;
    }

    /* Get the transaction ID and Nonce which is required while POSTING to OBP */
    public String[] getTxnIdAndNonce() {
        String[] txnIdAndNonce = new String[2];
        HttpsURLConnection con = null;
        BufferedReader in = null;
        try {
            /* Get URL */
            URL txnIdURL = new URL(OBPUtils.getInstance().getBlockchainPlatformURL(Constants.OBP_TXNID));
            /* Open Connection */
            con = (HttpsURLConnection) txnIdURL.openConnection();
            /* Set request method */
            con.setRequestMethod("GET");
            con.setRequestProperty("Accept", "application/json");
            /* Add Basic Authentication */
            con.setRequestProperty("Authorization", "Basic " + new String(AuthHeader.getInstance().getAuthHeaderValue()));
            int status = con.getResponseCode();
            /* If not 200 , then error */
            if (status != HttpsURLConnection.HTTP_OK) {
                try ( Reader streamReader = new InputStreamReader(con.getErrorStream())) {
                    System.err.println(OBPUtils.getInstance().errorReader(streamReader));
                }
            } else {
                /* Read the response */
                in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                JSONObject JSONResponse = new JSONObject(OBPUtils.getInstance().readResponse(in));
                JSONObject result = JSONResponse.getJSONObject("result");
                /*Txn Id and Nonce */
                txnIdAndNonce[0] = result.getString("txid");
                txnIdAndNonce[1] = result.getString("nonce");
            }
        } catch (MalformedURLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
        return txnIdAndNonce;
    }

    /* Publish Some data to the OBP */
    public void postData(String jsonBody) {
        HttpsURLConnection con = null;
        BufferedReader in = null;
        try {
            /* Get URL */
            URL postURL = new URL(OBPUtils.getInstance().getBlockchainPlatformURL(Constants.OBP_POST));
            /* Open Connection */
            con = (HttpsURLConnection) postURL.openConnection();
            /* Request Method */
            con.setRequestMethod("POST");
            /* Request Headers */
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setRequestProperty("Accept", "application/json");
            /* Basic Authentication */
            con.setRequestProperty("Authorization", "Basic " + new String(AuthHeader.getInstance().getAuthHeaderValue()));
            /* Expecting Response */
            con.setDoOutput(true);
            /* Add Body to request */
            try ( OutputStream os = con.getOutputStream()) {
                byte[] body_bytes = jsonBody.getBytes("utf-8");
                os.write(body_bytes, 0, body_bytes.length);
            }
            int status = con.getResponseCode();
            /* If not 201 , then error */
            if (status != HttpsURLConnection.HTTP_CREATED) {
                try ( Reader streamReader = new InputStreamReader(con.getErrorStream())) {
                    System.err.println(OBPUtils.getInstance().errorReader(streamReader));
                }
            } else {
                /* Read the response */
                in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                JSONObject JSONResponse = new JSONObject(OBPUtils.getInstance().readResponse(in));
                JSONObject result = JSONResponse.getJSONObject("result");
                System.out.println("Insert to OBP Sucessful!");
                /* Print Txn Id */
                System.out.println("Txn Id : " + result.getString("txid"));
            }
        } catch (MalformedURLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (NumberFormatException ex) {
            throw new Error("REST SERVER PORT MUST BE AN INTEGER");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
    }

    /* Read some data from the OBP */
    public JSONObject fetchData(String jsonBody) {
        JSONObject response = null;
        BufferedReader in = null;
        HttpsURLConnection con = null;
        try {
            URL readURL = new URL(OBPUtils.getInstance().getBlockchainPlatformURL(Constants.OBP_GET));
            /* Open Connection */
            con = (HttpsURLConnection) readURL.openConnection();
            /* Request Method */
            con.setRequestMethod("POST");
            /* Request Headers */
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setRequestProperty("Accept", "application/json");
            /* Basic Authentication */
            con.setRequestProperty("Authorization", "Basic " + new String(AuthHeader.getInstance().getAuthHeaderValue()));
            con.setDoOutput(true);
            /* Add Request Body */
            try ( OutputStream os = con.getOutputStream()) {
                byte[] body_bytes = jsonBody.getBytes("utf-8");
                os.write(body_bytes, 0, body_bytes.length);
            }
            int status = con.getResponseCode();
            /* If not 200 , then error */
            if (status != HttpsURLConnection.HTTP_OK) {
                try ( Reader streamReader = new InputStreamReader(con.getErrorStream())) {
                    System.err.println(OBPUtils.getInstance().errorReader(streamReader));
                }
            } else {
                /* Read the response */
                in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                JSONObject JSONResponse = new JSONObject(OBPUtils.getInstance().readResponse(in));
                /* Build response */
                JSONObject result = JSONResponse.getJSONObject("result");
                response = result.get("payload").equals("") ? new JSONObject() : result.getJSONObject("payload");
            }
        } catch (MalformedURLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (NumberFormatException ex) {
            System.err.println("REST SERVER PORT MUST BE AN INTEGER");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
        return response;
    }
}
