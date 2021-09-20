/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.sql.CharacterSet;
import oracle.sql.RAW;


public class DBUtils {

    private static DBUtils instance;
    private final HashMap<String, Integer> data_type_map = new HashMap<>();
    private static final Logger logger = Logger.getLogger(DBUtils.class.getName());
    private String db_guid;

    private DBUtils() {
        populateDataTypeMap();
    }

    public static DBUtils getDBUtils() {
        if (instance == null) {
            instance = new DBUtils();
        }
        return instance;
    }

    /**
     * Returns the code listed for data types internally used by Oracle Database
     *
     * @param datatype : Column Name
     * @return data type id
     */
    public int getDataTypeID(String datatype) {
        if (datatype.matches("TIMESTAMP(.*) WITH LOCAL TIME ZONE")) {
            return 231;
        } else if (datatype.matches("TIMESTAMP(.*) WITH TIME ZONE")) {
            return 181;
        } else if (datatype.matches("TIMESTAMP(.*)")) {
            return 180;
        } else if (datatype.matches("INTERVAL YEAR(.*) TO MONTH")) {
            return 182;
        } else if (datatype.matches("INTERVAL DAY(.*) TO SECOND(.*)")) {
            return 183;
        }
        return data_type_map.containsKey(datatype) ? (int)data_type_map.get(datatype) : 0;
    }

    /**
     * Converts a hex String to oracle.sql.RAW and get its String value
     *
     * @param hex - HEX String
     * @return oracle.sql.RAW
     */
    public String RAWToString(String hex) {
        RAW r = null;
        try {
            r = RAW.newRAW(hex);
        } catch (SQLException e) {
            throw new Error("Invalid HEX String");
        }
        return r.stringValue();
    }

    /**
     * Reads a NClob/Clob in a 4K char buffer and get AL16UTF16 Bytes and then
     * write it to the disk using the OutputStream.
     *
     * @param isClob - Are we reading a CLOB or NCLOB
     */
    public void writeClob(boolean isClob) {
        char[] char_buff = new char[4 * 1024];
        byte[] byte_buff = new byte[8 * 1024];
        int cRead;
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true);
             final Reader clob_reader = isClob ? IO.getIOInstance().getClob().getCharacterStream() : 
                                                 IO.getIOInstance().getNClob().getCharacterStream()) {
            while ((cRead = clob_reader.read(char_buff, 0, char_buff.length)) != -1) {
                CharacterSet.javaCharsToAL16UTF16Bytes(char_buff, cRead, byte_buff);
                byteStream.write(byte_buff, 0, cRead * 2);
            }
        } catch (SQLException | IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * Reads a blob into the buffer specified and then writes it to the disk
     * using the OutputStream.
     *
     */
    public void writeBlob() {
        /*Create a 8KB Buffer */
        byte buf[] = new byte[8 * 1024];
        int nRead;
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true);
             final InputStream blob_stream = IO.getIOInstance().getBlob().getBinaryStream()){
            /* Read till the end of Blob */
            while ((nRead = blob_stream.read(buf, 0, buf.length)) != -1) {
                byteStream.write(buf, 0, nRead);
            }
        } catch (SQLException | IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void populateDataTypeMap() {
        data_type_map.put(Constants.DB_VARCHAR, 1);
        data_type_map.put(Constants.DB_NVARCHAR, 1);
        data_type_map.put(Constants.DB_NUMBER, 2);
        data_type_map.put(Constants.DB_FLOAT, 2);
        data_type_map.put(Constants.DB_LONG, 8);
        data_type_map.put(Constants.DB_DATE, 12);
        data_type_map.put(Constants.DB_RAW, 23);
        data_type_map.put(Constants.DB_LONG_RAW, 24);
        data_type_map.put(Constants.DB_ROWID, 69);
        data_type_map.put(Constants.DB_CHAR, 96);
        data_type_map.put(Constants.DB_NCHAR, 96);
        data_type_map.put(Constants.DB_BINARY_FLOAT, 100);
        data_type_map.put(Constants.DB_BINARY_DOUBLE, 101);
        data_type_map.put(Constants.DB_CLOB, 112);
        data_type_map.put(Constants.DB_NCLOB, 112);
        data_type_map.put(Constants.DB_BLOB, 113);
        data_type_map.put(Constants.DB_BFILE, 114);
        data_type_map.put(Constants.DB_JSON, 119);
        data_type_map.put(Constants.DB_UROWID, 208);
    }
    
    public String getDbGUID() {
        if (db_guid == null) {
            Connection con = DBConnection.getInstance().getConnection();
            String guid_stmt = "select guid from v$containers";

            try ( Statement st = con.createStatement()) {
                try ( ResultSet rs = st.executeQuery(guid_stmt)) {
                    if (rs.next()) {
                        db_guid = rs.getString(1);
                    }
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        return db_guid;
    }
}
