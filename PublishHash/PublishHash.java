/* 
 * PublishHash Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class PublishHash {

    /**
     * Creates a new Connection to the Database and returns the Connection
     * Object.
     *
     * @param hostname - Oracle Hostname
     * @param port - DB Port
     * @param oracle_sid - Oracle sid
     * @return
     */
    public static Connection getConnection(String hostname, String port, String oracle_sid) {
        Connection con = null;
        try {
            /* Read Database credentials */
            BufferedReader credentials = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter Oracle Database Username:");
            final String DB_USERNAME = credentials.readLine();
            System.out.println("Enter Oracle Database Password:");
            final String DB_PASSWORD = credentials.readLine();
            con = DriverManager.getConnection(getJDBCUrl(hostname, Integer.parseInt(port), oracle_sid), DB_USERNAME, DB_PASSWORD);
        } catch (SQLException | IOException e) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, e);
        } catch (NumberFormatException ex) {
            System.err.println("PORT MUST BE AN INTEGER");
            System.exit(0);
        }
        return con;
    }

    /**
     * Formulate a JDBC String to connect to the Database which is consumed by
     * getConnection().
     *
     * @param hostname - Database hostname
     * @param port - DB port
     * @param oracle_sid - oracle_sid
     * @return JDBC Connection URL
     */
    static String getJDBCUrl(String hostname, Integer port, String oracle_sid) {
        return "jdbc:oracle:thin:@" + hostname + ":" + port + ":" + oracle_sid;
    }

    /**
     * Returns the Blockchain Platform URL Endpoint
     *
     * @param operation - read/write operation
     * @param rest_url - REST Server URL
     * @param rest_port - REST Server port
     * @param channel_id - Channel id
     * @return Blockchain Platform URL Endpoint
     */
    static String getBlockchainPlatformURL(String operation, String rest_url, Integer rest_port, String channel_id) {
        return "https://" + rest_url + ":" + rest_port + "/restproxy/api/v2/channels/" + channel_id + "/" + operation;
    }

    /**
     * Cleans the user input by removing unnecessary spaces and normalizing the
     * input for the database if not already enquoted.This method is called only
     * for user inputs such as table name or schema name. If the table name or
     * schema name is already enquoted return that string otherwise sanitize the
     * input and return enquoted literal.
     * User input strings can contain a pair of quotation marks placed at the 
     * beginning and end. Any other combination of quotation marks is invalid. 
     *
     * @param s - Input String
     * @return Cleaned String
     */
    private static String cleanString(Connection con , String s) {
        Statement st = null;
        String enquoted = null;
        try {
            st = con.createStatement();
            enquoted = st.enquoteIdentifier(s, false);
        } catch (SQLException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return enquoted;
    }

    /**
     * Reads the error if request fails.
     *
     * @param streamReader - Error Reader
     * @return - Error String
     */
    static String errorReader(Reader streamReader) {
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
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        }
        return errorString;
    }

    /**
     * Reads Response for the request.
     *
     * @param in - Response InputStreamReader.
     * @return - Response String
     */
    static String readResponse(BufferedReader in) {
        StringBuilder response = new StringBuilder();
        try {
            String inputLine;
            /* Read till the end of Stream */
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        } catch (IOException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        }
        return response.toString();
    }

    /**
     * Gets Transaction ID and Nonce to make a request to the Oracle Blockchain
     * Platform
     *
     * @param rest_url - REST Server URL
     * @param rest_port - REST Server port
     * @param channel_id - Channel id
     * @return Txn Id & Nonce
     */
    static String[] getTxnIdAndNonce(String rest_url, Integer rest_port, String channel_id) {
        String[] txnIdAndNonce = new String[2];
        HttpsURLConnection con = null;
        BufferedReader in = null;
        try {
            /* Get URL */
            URL txnIdURL = new URL(getBlockchainPlatformURL("transaction-id", rest_url, rest_port, channel_id));
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
                    System.err.println(errorReader(streamReader));
                }
            } else {
                /* Read the response */
                in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                JSONObject JSONResponse = new JSONObject(readResponse(in));
                JSONObject result = JSONResponse.getJSONObject("result");
                /*Txn Id and Nonce */
                txnIdAndNonce[0] = result.getString("txid");
                txnIdAndNonce[1] = result.getString("nonce");
            }
        } catch (MalformedURLException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
        return txnIdAndNonce;
    }

    /**
     * Builds JSON Body to send with the request.
     *
     * @param schema_name - Schema name
     * @param table_name - Table name
     * @param instance_id - Instance id
     * @param chain_id - Chain id
     * @param sequence_id - Sequence id
     * @param hash - hash to publish
     * @param rest_url - REST Server URL
     * @param rest_port - REST Server port
     * @param channel_id - Channel id
     * @param chaincode_name - Chaincode name deployed on Blockchain Platform
     * @return - JSON Body Built
     */
    static String JSONBodyBuilder(String schema_name, String table_name, int instance_id, int chain_id, int sequence_id, String hash, String rest_url, Integer rest_port, String channel_id, String chaincode_name) {
        /* Get Txn Id and Nonce */
        String[] txnIdAndNonce = getTxnIdAndNonce(rest_url, rest_port, channel_id);
        /* Build Body for Request */
        JSONObject body = new JSONObject();
        /* Txn ID */
        body.put("txid", txnIdAndNonce[0]);
        /* Nonce */
        body.put("nonce", txnIdAndNonce[1]);
        /* Chaincode to invoke */
        body.put("chaincode", chaincode_name);
        /* If Hash is present we want to call storeHash() else call readHash() */
        JSONArray argsArray = new JSONArray();
        if (hash != null) {
            argsArray.put("storeHash");
        } else {
            argsArray.put("readHash");
        }
        /* Method arguments to pass */
        argsArray.put(schema_name);
        argsArray.put(table_name);
        argsArray.put(String.valueOf(instance_id));
        argsArray.put(String.valueOf(chain_id));
        argsArray.put(String.valueOf(sequence_id));
        if (hash != null) {
            argsArray.put(hash);
        }
        body.put("args", argsArray);
        body.put("timeout", 18000);
        body.put("sync", true);
        return body.toString();
    }

    /**
     * Publish Data to Oracle Blockchain Platform
     *
     * @param schema_name - Schema name
     * @param table_name - Table name
     * @param instance_id - Instance id
     * @param chain_id - Chain Id
     * @param sequence_id - Sequence Id
     * @param hash - Hash to publish
     * @param rest_url - Rest URL
     * @param rest_port - Rest Port
     * @param channel_id - Channel Id
     * @param chaincode_name - Chaincode deployed on Oracle Blockchain Platform
     */
    static void postData(String schema_name, String table_name, int instance_id, int chain_id, int sequence_id, String hash, String rest_url, String rest_port, String channel_id, String chaincode_name){
        HttpsURLConnection con = null;
        BufferedReader in = null;
        try {
            /* Get URL */
            URL postURL = new URL(getBlockchainPlatformURL("transactions", rest_url, Integer.parseInt(rest_port), channel_id));
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
                byte[] body_bytes = JSONBodyBuilder(schema_name, table_name, instance_id, chain_id, sequence_id, hash, rest_url, Integer.parseInt(rest_port), channel_id, chaincode_name).getBytes("utf-8");
                os.write(body_bytes, 0, body_bytes.length);
            }
            int status = con.getResponseCode();
            /* If not 201 , then error */
            if (status != HttpsURLConnection.HTTP_CREATED) {
                try ( Reader streamReader = new InputStreamReader(con.getErrorStream())) {
                    System.err.println(errorReader(streamReader));
                }
            } else {
                /* Read the response */
                in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                JSONObject JSONResponse = new JSONObject(readResponse(in));
                JSONObject result = JSONResponse.getJSONObject("result");
                System.out.println("Insert Sucessful!");
                /* Print Txn Id */
                System.out.println("Txn Id : " + result.getString("txid"));
            }
        } catch (MalformedURLException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NumberFormatException ex) {
            throw new Error("REST SERVER PORT MUST BE AN INTEGER");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
    }

    /**
     * Reads hash from the Oracle Blockchain Platform.
     *
     * @param schema_name - Schema Name
     * @param table_name - Table Name
     * @param instance_id - Instance ID
     * @param chain_id - Chain ID
     * @param sequence_id - Sequence number
     * @return - Stored hash
     */
    static String readHash(String schema_name, String table_name, int instance_id, int chain_id, int sequence_id) {
        String hash = "";
        BufferedReader in = null;
        HttpsURLConnection con = null;
        try {
            Properties prop;
            URL readURL;
            /* Read config file */ 
            try (InputStream input = new FileInputStream("config.properties")) {
                prop = new Properties();
                /* Load properties */
                prop.load(input);
                /* Get URL */
                readURL = new URL(getBlockchainPlatformURL("chaincode-queries", prop.getProperty("rest_server_url"), Integer.parseInt(prop.getProperty("rest_server_port")), prop.getProperty("channel_id")));
            }
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
                byte[] body_bytes = JSONBodyBuilder(schema_name, table_name, instance_id, chain_id, sequence_id, null, prop.getProperty("rest_server_url"), Integer.parseInt(prop.getProperty("rest_server_port")), prop.getProperty("channel_id"), prop.getProperty("chaincode_name")).getBytes("utf-8");
                os.write(body_bytes, 0, body_bytes.length);
            }
            int status = con.getResponseCode();
            /* If not 200 , then error */
            if (status != HttpsURLConnection.HTTP_OK) {
                try ( Reader streamReader = new InputStreamReader(con.getErrorStream())) {
                    System.err.println(errorReader(streamReader));
                }
            } else {
                /* Read the response */
                in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                JSONObject JSONResponse = new JSONObject(readResponse(in));
                JSONObject result = JSONResponse.getJSONObject("result");
                /* Read the hash */
                hash = result.get("payload").toString();
            }
        } catch (MalformedURLException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NumberFormatException ex) {
            System.err.println("REST SERVER PORT MUST BE AN INTEGER");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                con.disconnect();
            }
        }
        return hash;
    }

    /**
     * Get the current row hash using instance_id , chain_id , seq_id for a
     * blockchain table t , in schema s.
     *
     * @param schema_name - User's Schema
     * @param table_name - Blockchain Table name
     * @param instance_id - instance id of the row
     * @param chain_id - chain id of the row
     * @param sequence_id - sequence id of the row
     * @param port - DB Port
     * @param oracle_sid - Oracle sid
     * @return - current_row_hash : Hash for the current row identified by
     * (instance_id , chain_id , sequence_id).
     */
    static String getCurrentRowHash(String schema_name, String table_name, int instance_id, int chain_id, int sequence_id, String hostname, String port, String oracle_sid) {
        Connection con = getConnection(hostname, port, oracle_sid);
        String schema_name_int = cleanString(con, schema_name);
        String table_name_int = cleanString(con, table_name);
        String current_row_hash = null;
        PreparedStatement col_val_stmt = null;
        ResultSet rs = null;
        /* SQL query to fetch the current row hash */
        String current_hash_qry = "SELECT ORABCTAB_HASH$ from " + schema_name_int + "." + table_name_int + " "
                + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and " + "ORABCTAB_SEQ_NUM$ = ?";
        try {
            col_val_stmt = con.prepareStatement(current_hash_qry);
            /* bind instance_id */
            col_val_stmt.setInt(1, instance_id);
            /* bind chain_id */
            col_val_stmt.setInt(2, chain_id);
            /* bind sequence_id */
            col_val_stmt.setInt(3, sequence_id);
            /* execute the query */
            rs = col_val_stmt.executeQuery();
            while (rs.next()) {
                /* get the current row hash */
                current_row_hash = rs.getString(1);
            }
        } catch (SQLException e) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (col_val_stmt != null) {
                try {
                    col_val_stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        /* return the current row hash */
        return current_row_hash;
    }

    /**
     * Gets Row Hash from the Database and Publishes to Oracle Blockchain
     * Platform.
     *
     * @param schema_name - Schema Name
     * @param table_name - Table Name
     * @param instance_id - Instance ID
     * @param chain_id - Chain ID
     * @param sequence_id - Sequence number
     */
    public static void publishHash(String schema_name, String table_name, int instance_id, int chain_id, int sequence_id) {
        InputStream input = null;
        try {
            /* Read config file */
            Properties prop = new Properties();
            input = new FileInputStream("config.properties");
            /* Load properties */
            prop.load(input);
            /* Get Hash from DB */
            String curr_hash = getCurrentRowHash(schema_name, table_name, instance_id, chain_id, sequence_id, prop.getProperty("hostname"), prop.getProperty("port"), prop.getProperty("oracle_sid"));
            /* Publish hash to Oracle Blockchain Platform */
            postData(schema_name, table_name, instance_id, chain_id, sequence_id, curr_hash, prop.getProperty("rest_server_url"), prop.getProperty("rest_server_port"), prop.getProperty("channel_id"), prop.getProperty("chaincode_name"));
            /* Print the hash published */
            System.out.println("Published Hash : " + curr_hash);
        } catch (IOException ex) {
            Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    Logger.getLogger(PublishHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Invalid Number Of Arguments Supplied.");
            System.err.println("Java program should be run using :");
            System.err.println("java Verify_Rows <OPERATION> <SCHEMA> <TABLE> <INSTANCE_ID> <CHAIN_ID> <SEQUENCE_NO>");
            System.exit(0);
        }
        try {
            final String OPERATION = args[0];
            final String SCHEMA = args[1];
            final String TABLE = args[2];
            final int INSTANCE_ID = Integer.parseInt(args[3]);
            final int CHAIN_ID = Integer.parseInt(args[4]);
            final int SEQUENCE_NO = Integer.parseInt(args[5]);
            if (OPERATION.equalsIgnoreCase("read")) {
                String hash = readHash(SCHEMA, TABLE, INSTANCE_ID, CHAIN_ID, SEQUENCE_NO);
                System.out.println(hash);
            } else if (OPERATION.equalsIgnoreCase("write")) {
                publishHash(SCHEMA, TABLE, INSTANCE_ID, CHAIN_ID, SEQUENCE_NO);
            } else {
                System.err.println("Invalid Operation. Operations can be read/write");
            }
        } catch (NumberFormatException ex) {
            System.err.println("INSTANCE_ID , CHAIN_ID , SEQUENCE_NO MUST BE AN INTEGER");
            System.exit(0);
        }
    }
}
