/* 
 * Verify_Rows Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.sql.CharacterSet;
import oracle.sql.RAW;

public class Verify_Rows {

    /**
     * Class to store Hash Column Information and maintain previous row hash for
     * faster verification.
     */
    static class HashColumnData {

        String column_type;
        Integer col_pos;
        String prev_hash;
        String curr_hash;

        HashColumnData(String column_type, Integer col_pos) {
            this.column_type = column_type;
            this.col_pos = col_pos;
            this.prev_hash = null;
            this.curr_hash = null;
        }
    }

    /**
     * Class to store column name , column type and column position of each
     * column required to calculate hash.
     */
    static class ColumnData {

        String column_name;
        String column_type;
        Integer column_pos;

        public ColumnData(String column_name, String column_type, Integer column_pos) {
            this.column_name = column_name;
            this.column_type = column_type;
            this.column_pos = column_pos;
        }

    }

    /**
     * Maintain Instance Level Stats.
     */
    static class InstanceStats {

        Integer success_count;
        Integer fail_count;

        public InstanceStats() {
            success_count = 0;
            fail_count = 0;
        }
    }

    /**
     * Maintain global stats.
     */
    static class GlobalStats {

        Integer success_count;
        Integer fail_count;

        public GlobalStats() {
            success_count = 0;
            fail_count = 0;
        }
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
    private static String cleanString(String s, boolean alwaysEnquote) {
        Statement st = null;
        String enquoted = null;
        try {
            Connection con = DBConnection.getInstance().getConnection();
            st = con.createStatement();
            enquoted = st.enquoteIdentifier(s, alwaysEnquote);
        } catch (SQLException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return enquoted;
    }

    /**
     * Returns the un-enquoted version of the user input for entities like
     * tables , schema by removing the quotes
     *
     * @param s - Input String
     * @return Unenquoted String
     */
    private static String unEnquoted(String s) {
        if (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Returns the file extension to copy the failed file into a new file using
     * same extension
     *
     * @param file - File to fetch extension
     * @return
     */
    static String getFileExtension(File file) {
        String name = file.getName();
        if (name.lastIndexOf(".") != -1 & name.lastIndexOf(".") != 0) {
            return name.substring(name.lastIndexOf(".") + 1);
        } else {
            return "";
        }
    }

    /**
     * Returns the code listed for data types internally used by Oracle Database
     *
     * @param datatype : Column Name
     * @return data type id
     */
    static int getDataTypeID(String datatype) {
        if (datatype.equals("VARCHAR2") || datatype.equals("NVARCHAR2")) {
            return 1;
        } else if (datatype.equals("NUMBER") || datatype.equals("FLOAT")) {
            return 2;
        } else if (datatype.equals("LONG")) {
            return 8;
        } else if (datatype.equals("DATE")) {
            return 12;
        } else if (datatype.equals("RAW")) {
            return 23;
        } else if (datatype.equals("LONG RAW")) {
            return 24;
        } else if (datatype.equals("ROWID")) {
            return 69;
        } else if (datatype.equals("CHAR") || datatype.equals("NCHAR")) {
            return 96;
        } else if (datatype.equals("BINARY_FLOAT")) {
            return 100;
        } else if (datatype.equals("BINARY_DOUBLE")) {
            return 101;
        } else if (datatype.equals("CLOB") || datatype.equals("NCLOB")) {
            return 112;
        } else if (datatype.equals("BLOB")) {
            return 113;
        } else if (datatype.equals("BFILE")) {
            return 114;
        } else if (datatype.equals("JSON")) {
            return 119;
        } else if (datatype.matches("TIMESTAMP(.*) WITH LOCAL TIME ZONE")) {
            return 231;
        } else if (datatype.matches("TIMESTAMP(.*) WITH TIME ZONE")) {
            return 181;
        } else if (datatype.matches("TIMESTAMP(.*)")) {
            return 180;
        } else if (datatype.matches("INTERVAL YEAR(.*) TO MONTH")) {
            return 182;
        } else if (datatype.equals("INTERVAL DAY(.*) TO SECOND(.*)")) {
            return 183;
        } else if (datatype.equals("UROWID")) {
            return 208;
        }
        return 0;
    }

    /**
     * Converts a hex String to oracle.sql.RAW and get its String value
     *
     * @param hex - HEX String
     * @return oracle.sql.RAW
     */
    static String RAWToString(String hex) {
        RAW r = null;
        try {
            r = RAW.newRAW(hex);
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
        return r == null ? "" : r.stringValue();
    }

    /**
     * Reads a Clob character by character and get AL16UTF16 Bytes and then
     * write it to the disk using the OutputStream.
     *
     * @param temp_reader - CLOB/NCLOB Reader
     * @param os - OutputStream to write to disk
     */
    static void writeClob(Reader temp_reader, OutputStream os) {
        try {
            int data = temp_reader.read();
            /* Read till end of the Clob */
            while (data != -1) {
                /* Current character */
                char[] temp_char = {(char) data};
                /* Byte buffer for the current character */
                byte[] temp_bytes = new byte[temp_char.length * 2];
                /* get AL16UTF16 bytes for current character */
                CharacterSet.javaCharsToAL16UTF16Bytes(temp_char, temp_char.length, temp_bytes);
                /* append to buffer */
                os.write(temp_bytes);
                data = temp_reader.read();
            }
        } catch (IOException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        } 
    }

    /**
     * Reads a blob into the buffer specified and then writes it to the disk
     * using the OutputStream.
     *
     * @param is - BLOB InputStream
     * @param os - OutputStream to write to disk
     */
    static void writeBlob(InputStream is, OutputStream os) {
        /*Create a 8KB Buffer */
        byte buf[] = new byte[8 * 1024];
        int nRead;
        try {
            /* Read till the end of Blob */
            while ((nRead = is.read(buf, 0, buf.length)) != -1) {
                os.write(buf, 0, nRead);
            }
        } catch (IOException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * Writes the spare column data if not null.
     *
     * @param sign_algo_pos - Signature algorithm column position
     * @param cert_id_pos - Certificate id column position
     * @param sign_algo - Signature Algorithm
     * @param cert_id - Certificate ID
     * @param spare_col - Spare column data
     * @param os - OutputStream to write to disk
     */
    static void writeSpareColumnData(Integer sign_algo_pos, Integer cert_id_pos, byte[] sign_algo, byte[] cert_id, byte[] spare_col, OutputStream os) {
        /* Get spare column as int value */
        Integer spare_int = ByteBuffer.wrap(spare_col).order(ByteOrder.LITTLE_ENDIAN).getInt();
        try {
            /* ORABCTAB_SIGNATURE_ALG$ has been set */
            if ((spare_int & 1) == 1) {
                os.write(populateMetadata(sign_algo_pos, "NUMBER", 0, sign_algo.length));
                os.write(sign_algo);
            }
            /* ORABCTAB_SIGNATURE_CERT$ has been set */
            if ((spare_int & 2) == 2) {
                os.write(populateMetadata(cert_id_pos, "RAW", 0, cert_id.length));
                os.write(cert_id);
            }
        } catch (IOException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    /**
     * Writes LOBS(CLOB , NCLOB , BLOB) type columns to disk
     *
     * @param con - Connection Object
     * @param column_name_quoted - Column name
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param instance_id - Instance id
     * @param chain_id - Chain id
     * @param sequence_id - Sequence number
     * @param column_type - Column type
     * @param col_pos - Column position
     * @param os - OutputStream to write to disk
     */
    static void writeLOBs(Connection con, String column_name_quoted, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, String column_type, int col_pos, OutputStream os) {
        /* Reader to read CLOB/NCLOB */
        Reader clob_reader = null;
        /* InputStream to read BLOB */
        InputStream blob_stream = null;
        /* length of the column - 8 BYTE VALUE */
        long column_length = 0;
        /* is the column null? - 1 BYTE VALUE */
        int column_isnull = 0;
        /* Query to fetch column data */
        String col_val_query = "select " + column_name_quoted + " " + "from" + " " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and "
                + "ORABCTAB_SEQ_NUM$ = ?";
        PreparedStatement col_val_stmt = null;
        ResultSet col_rs = null;
        try {
            col_val_stmt = con.prepareStatement(col_val_query);
            /* bind instance_id */
            col_val_stmt.setInt(1, instance_id);
            /* bind chain_id */
            col_val_stmt.setInt(2, chain_id);
            /* bind sequence_id */
            col_val_stmt.setInt(3, sequence_id);
            /* execute the query */
            col_rs = col_val_stmt.executeQuery();
            if (col_rs.next()) {
                if (column_type.equals("CLOB")) {
                    /* fetch the data inside java.sql.Clob */
                    Clob temp_val = col_rs.getClob(1);
                    /* column length */
                    column_length = temp_val == null ? 0 : temp_val.length() * 2;
                    /* check for null value of Clob */
                    if (temp_val == null) {
                        column_isnull = 1;
                    } else {
                        /* get the Character Stream of the Clob Value */
                        clob_reader = temp_val.getCharacterStream();
                    }
                } else if (column_type.equals("NCLOB")) {
                    /* fetch the data inside java.sql.NClob */
                    NClob temp_val = col_rs.getNClob(1);
                    /* column length */
                    column_length = temp_val == null ? 0 : temp_val.length() * 2;
                    /* check for null value of NClob */
                    if (temp_val == null) {
                        column_isnull = 1;
                    } else {
                        /* get the Character Stream of the NClob Value */
                        clob_reader = temp_val.getCharacterStream();
                    }
                } else {
                    /* fetch the data inside java.sql.Blob */
                    Blob temp_val = col_rs.getBlob(1);
                    /* column length */
                    column_length = temp_val == null ? 0 : temp_val.length();
                    /* check for null value of Blob */
                    if (temp_val == null) {
                        column_isnull = 1;
                    } else {
                        /* get the Byte Stream of the Blob Value */
                        blob_stream = temp_val.getBinaryStream();
                    }
                }
            }
            /* append metadata to main buffer for all columns */
            os.write(populateMetadata(col_pos, column_type, column_isnull,
                    column_length));
            /* append column value if not null */
            if (column_length != 0) {
                if (column_type.equals("BLOB")) {
                    /* Read BLOB using InputStream , then write to disk */
                    writeBlob(blob_stream, os);
                } else if (column_type.equals("CLOB") || column_type.equals("NCLOB")) {
                    /* Read CLOB/NCLOB character by character and get AL16UTF16 byte representation , then write to disk*/
                    writeClob(clob_reader, os);
                }
            }
        } catch (SQLException | IOException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if(col_rs != null) {
                try {
                    col_rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if(col_val_stmt != null) {
                try {
                    col_val_stmt.close();
                } catch (SQLException ex) {
                    Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (clob_reader != null) {
                try {
                    clob_reader.close();
                } catch (IOException ex) {
                    Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (blob_stream != null) {
                try {
                    blob_stream.close();
                } catch (IOException ex) {
                    Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /**
     * Writes Strings(VARCHAR2 , NVARCHAR2 , CHAR , NCHAR) type columns to disk.
     *
     * @param con - Connection Object
     * @param column_name_quoted - Column name
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param instance_id - instance id
     * @param chain_id - chain id
     * @param sequence_id - sequence number
     * @param column_type - column type
     * @param col_pos - column position
     * @param os - OutputStream to write to disk
     */
    static void writeStrings(Connection con, String column_name_quoted, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, String column_type, int col_pos, OutputStream os) {
        try {
            /* is the column null? - 1 BYTE VALUE */
            int column_isnull = 0;
            /* length of the column - 8 BYTE VALUE */
            long column_length = 0;
            /* temporary buffer for column value */
            byte[] temp_bytes = null;
            /* Query to fetch column data */
            String col_val_query = "select " + column_name_quoted + " " + "from" + " " + schema_name_int + "."
                    + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and "
                    + "ORABCTAB_SEQ_NUM$ = ?";
            try ( PreparedStatement col_val_stmt = con.prepareStatement(col_val_query)) {
                /* bind instance_id */
                col_val_stmt.setInt(1, instance_id);
                /* bind chain_id */
                col_val_stmt.setInt(2, chain_id);
                /* bind sequence_id */
                col_val_stmt.setInt(3, sequence_id);
                /* execute the query */
                try ( ResultSet col_rs = col_val_stmt.executeQuery()) {
                    while (col_rs.next()) {
                        /* fetch the data inside String */
                        String temp_val = (column_type.equals("VARCHAR2") || column_type.equals("CHAR"))
                                ? col_rs.getString(1)
                                : col_rs.getNString(1);
                        /* check for null value of String */
                        if (temp_val == null) {
                            column_isnull = 1;
                        } else {
                            if (column_type.equals("CHAR") || column_type.equals("NCHAR")) {
                                /* trim blanks except for one blank in an all-blank value */
                                temp_val = temp_val.trim();
                                if (temp_val.length() == 0) {
                                    temp_val = " ";
                                }
                            }
                            if (column_type.equals("VARCHAR2") || column_type.equals("CHAR")) {
                                /* Normalize: fetch the AL32UTF8 bytes */
                                temp_bytes = CharacterSet.stringToAL32UTF8(temp_val);
                            } else if (column_type.equals("NVARCHAR2") || column_type.equals("NCHAR")) {
                                /* Normalize: fetch the AL16UTF16 bytes */
                                temp_bytes = CharacterSet.stringToAL16UTF16Bytes(temp_val);
                            }
                        }
                    }
                    column_length = temp_bytes == null ? 0 : temp_bytes.length;
                    /* append metadata to main buffer for all columns */
                    os.write(populateMetadata(col_pos, column_type, column_isnull,
                            column_length));
                    if (column_length != 0) {
                        /* Write to Disk */
                        os.write(temp_bytes);
                    }
                }
            }
        } catch (SQLException | IOException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Writes Other Scalar type columns to disk.
     *
     * @param con - Connection Object
     * @param column_name_quoted - Column name
     * @param schema_name_int - Schema Name
     * @param table_name_int - Table Name
     * @param instance_id - Instance Id
     * @param chain_id - Chain Id
     * @param sequence_id - Sequence Number
     * @param column_type - Column Type
     * @param col_pos - Column Position
     * @param os - OutputStream to write to disk.
     */
    static void writeScalar(Connection con, String column_name_quoted, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, String column_type, int col_pos, OutputStream os) {
        try {
            /* temporary buffer for column value */
            byte[] temp_bytes = null;
            /* is the column null? - 1 BYTE VALUE */
            int column_isnull = 0;
            /* length of the column - 8 BYTE VALUE */
            long column_length = 0;
            /* Query to fetch column data */
            String col_val_query = null;
            if (column_type.equals("JSON")) {
                col_val_query = "select OSON_GET_CONTENT(" + column_name_quoted + ") " + "from" + " " + schema_name_int
                        + "." + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and "
                        + "ORABCTAB_CHAIN_ID$ = ? and " + "ORABCTAB_SEQ_NUM$ = ?";
            } else {
                col_val_query = "select " + column_name_quoted + " " + "from" + " " + schema_name_int + "."
                        + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and "
                        + "ORABCTAB_CHAIN_ID$ = ? and " + "ORABCTAB_SEQ_NUM$ = ?";
            }
            try ( PreparedStatement col_val_stmt = con.prepareStatement(col_val_query)) {
                /* bind instance_id */
                col_val_stmt.setInt(1, instance_id);
                /* bind chain_id */
                col_val_stmt.setInt(2, chain_id);
                /* bind sequence_id */
                col_val_stmt.setInt(3, sequence_id);
                /* execute the query */
                try ( ResultSet col_rs = col_val_stmt.executeQuery()) {
                    while (col_rs.next()) {
                        /* fetch bytes for column value */
                        temp_bytes = col_rs.getBytes(1);
                        /* check for null column value */
                        if (col_rs.wasNull()) {
                            column_isnull = 1;
                        }
                    }
                    column_length = temp_bytes == null ? 0 : temp_bytes.length;
                    /* append metadata to main buffer for all columns */
                    os.write(populateMetadata(col_pos, column_type, column_isnull,
                            column_length));
                    if (column_length != 0) {
                        /* Write to Disk */
                        os.write(temp_bytes);
                    }
                }
            }
        } catch (SQLException | IOException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Populate metadata for columns and return bytes
     *
     * @param col_pos - Column Position
     * @param column_type - Column Type
     * @param column_isnull - Column isNull
     * @param column_length - Column Length
     * @return - Metadata byte[].
     */
    static byte[] populateMetadata(int col_pos, String column_type, int column_isnull, long column_length) {
        /* column_version - 2 bytes value - VALUE IS ALWAYS 1 */
        byte[] col_version = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) 1).array();
        /* column reserved - 1 byte value - VALUE IS ALWAYS 0 */
        byte[] col_reserved = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN).put((byte) 0).array();
        /* column spare - 4 byte value - VALUE IS 0 FOR THIS RELEASE */
        byte[] col_spare = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).array();
        /* temporary buffer for column metadata */
        ByteArrayOutputStream temp_metadata = new ByteArrayOutputStream();
        /* get internal data type code for column_type */
        int column_type_int = getDataTypeID(column_type);
        /* column position - 2 byte value - VALUE IS TAKEN FROM col_pos */
        byte[] col_position = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) col_pos).array();
        /* column type - 2 byte value - VALUE IS TAKEN FROM column_type_int */
        byte[] col_type = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) column_type_int)
                .array();
        /* column isnull - 1 byte value - VALUE IS TAKEN FROM column_isnull */
        byte[] col_isnull = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN).put((byte) column_isnull).array();
        /* column length - 8 byte value - VALUE IS TAKEN FROM column_length */
        byte[] col_len = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(column_length).array();
        /*
         * Formulate metadata for this column(Total 20 Bytes). The order for appending
         * data is : 1. Column Version 2. Column Position 3. Column Type 4. Column
         * IsNull? 5. Column Reserved 6. Column Length 7. Column Spare
         */
        try {
            temp_metadata.write(col_version);
            temp_metadata.write(col_position);
            temp_metadata.write(col_type);
            temp_metadata.write(col_isnull);
            temp_metadata.write(col_reserved);
            temp_metadata.write(col_len);
            temp_metadata.write(col_spare);
        } catch (IOException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                temp_metadata.close();
            } catch (IOException ex) {
                Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return temp_metadata.toByteArray();
    }

    /**
     * Encrypt the row-bytes got using getBytesForRowHash() using SHA-512
     * scheme.
     *
     * @param file_path - Path to row bytes file
     * @return - SHA-512 Encrypted Hash
     */
    private static String encryptSHA512(String file_path) {
        String ht = null;
        InputStream is = null;
        try {
            /* getInstance() called with SHA-512 Algorithm */
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            /* calculate the message digest after getting row-bytes */
            is = new FileInputStream(file_path);
            /* Creating 8KB buffer */
            /* DigestInputStream for calculating hashing over a stream of bytes */
            try ( DigestInputStream di = new DigestInputStream(is, md)) {
                /* Creating 8KB buffer */
                byte buf[] = new byte[8 * 1024];
                while (di.read(buf) != -1) {
                    ;
                }
            }
            /* get the digest */
            byte[] messageDigest = md.digest();
            /* convert the byte[] to signum representation */
            BigInteger no = new BigInteger(1, messageDigest);
            /* convert digest into hex value */
            ht = no.toString(16);
            /* Add 0s to make it 128-bit */
            while (ht.length() < 128) {
                ht = "0" + ht;
            }
        } catch (NoSuchAlgorithmException | FileNotFoundException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        } catch (IOException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        /* convert to oracle.sql.RAW and return the SHA-512 hash */
        return RAWToString(ht);
    }

    /**
     * Stores the hash column data type and position for faster verification.
     *
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @return - Hash Column Data type and position.
     */
    static HashColumnData getHashColumnData(String schema_name_int, String table_name_int) {
        Connection con = DBConnection.getInstance().getConnection();
        HashColumnData data = null;
        boolean null_check = true;
        /* Query to fetch the previous row hash metadata */
        String prev_row_hash_metadata_qry = "select data_type, internal_column_id from SYS.ALL_TAB_COLS "
                + "where OWNER = ? and TABLE_NAME = ? and " + "COLUMN_NAME = 'ORABCTAB_HASH$'";
        try {
            try ( PreparedStatement prev_row_hash_metadata_stmt = con.prepareStatement(prev_row_hash_metadata_qry)) {
                /* bind schema_name */
                prev_row_hash_metadata_stmt.setString(1, unEnquoted(schema_name_int));
                /* bind table_name */
                prev_row_hash_metadata_stmt.setString(2, unEnquoted(table_name_int));
                /* execute the query */
                try ( ResultSet rs = prev_row_hash_metadata_stmt.executeQuery()) {
                    while (rs.next()) {
                        null_check = !null_check;
                        data = new HashColumnData(rs.getString(1), rs.getInt(2));
                    }
                    if (null_check) {
                        System.err.println("Schema or Table name not found!");
                        DBConnection.getInstance().closeConnection();
                        System.exit(0);
                    }
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
        return data;
    }

    /**
     * Get Column data - Column Name , Column Type & Column Position for all
     * columns required to calculate hash.
     *
     * @param schema_name_int - Schema Name
     * @param table_name_int - Table Name
     * @return Column data
     */
    static List<ColumnData> getColumnData(String schema_name_int, String table_name_int) {
        /*
         * Query to fetch column name , data type and internal column id to calculate
         * metadata
         */
        Connection con = DBConnection.getInstance().getConnection();
        String column_names_query = "select column_name, data_type, internal_column_id from SYS.ALL_TAB_COLS "
                + "where OWNER = ? and TABLE_NAME = ? and "
                + "COLUMN_NAME NOT IN ('ORABCTAB_HASH$','ORABCTAB_SIGNATURE$')"
                + "and VIRTUAL_COLUMN='NO' order by INTERNAL_COLUMN_ID";
        /* Data for all columns */
        List<ColumnData> column_list = new ArrayList<>();
        try {
            try ( PreparedStatement col_names_stmt = con.prepareStatement(column_names_query)) {
                /* bind schema_name */
                col_names_stmt.setString(1, unEnquoted(schema_name_int));
                /* bind table_name */
                col_names_stmt.setString(2, unEnquoted(table_name_int));
                /* execute query */
                try ( ResultSet rs = col_names_stmt.executeQuery()) {
                    while (rs.next()) {
                        /* New column */
                        ColumnData data = new ColumnData(rs.getString(1), rs.getString(2), rs.getInt(3));
                        /* Add to the list */
                        column_list.add(data);
                    }
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
        return column_list;
    }

    /**
     * Get the row hash using instance_id , chain_id , seq_id for a blockchain
     * table t , in schema s.
     *
     * @param schema_name_int - User's Schema
     * @param table_name_int - Blockchain Table name
     * @param instance_id - instance id of the row
     * @param chain_id - chain id of the row
     * @param sequence_id - sequence id of the row
     * @return - current_row_hash : Hash for the current row identified by
     * (instance_id , chain_id , sequence_id).
     */
    public static String getRowHash(String schema_name_int, String table_name_int, int instance_id, int chain_id,
            int sequence_id) {
        Connection con = DBConnection.getInstance().getConnection();
        String current_row_hash = null;
        /* SQL query to fetch the current row hash */
        String current_hash_qry = "SELECT ORABCTAB_HASH$ from " + schema_name_int + "." + table_name_int + " "
                + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and " + "ORABCTAB_SEQ_NUM$ = ?";
        try {
            try ( PreparedStatement col_val_stmt = con.prepareStatement(current_hash_qry)) {
                /* bind instance_id */
                col_val_stmt.setInt(1, instance_id);
                /* bind chain_id */
                col_val_stmt.setInt(2, chain_id);
                /* bind sequence_id */
                col_val_stmt.setInt(3, sequence_id);
                /* execute the query */
                try ( ResultSet rs = col_val_stmt.executeQuery()) {
                    while (rs.next()) {
                        /* get the current row hash */
                        current_row_hash = rs.getString(1);
                    }
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
        /* return the current row hash */
        return current_row_hash;
    }

    /**
     * Report any discrepancy found in sequence numbers.
     *
     * @param prev_seq_no - Previous Sequence No.
     * @param curr_seq_no - Current Sequence No.
     * @return - if sequence number ordering is valid
     */
    static boolean verifySequence(Integer prev_seq_no, Integer curr_seq_no) {
        if (prev_seq_no == null) {
            return true;
        }
        return !(curr_seq_no == null || curr_seq_no <= 0 || prev_seq_no.equals(curr_seq_no) || curr_seq_no - prev_seq_no != 1);
    }

    /**
     * Verify all rows in all instances.
     *
     * @param file - File to write temp bytes
     * @param filepath - File path
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param hashColumnData - Hash Column Data and previous hash information.
     * @param COPY_BYTESFILE_FOR_FAILED - Copy bytes file if hash verification
     * failed.
     */
    private static void verifyAllInstances(File file, String filepath, String schema_name_int, String table_name_int, HashColumnData hashColumnData, List<ColumnData> columnData, boolean COPY_BYTESFILE_FOR_FAILED) {
        Connection con = DBConnection.getInstance().getConnection();
        /* get all instances */
        String instance_id_qry = "select DISTINCT ORABCTAB_INST_ID$ from " + schema_name_int + "." + table_name_int + " ORDER BY ORABCTAB_INST_ID$";
        GlobalStats globalStats = new GlobalStats();
        try {
            try ( PreparedStatement instance_id_stmt = con.prepareStatement(instance_id_qry);  ResultSet instance_rs = instance_id_stmt.executeQuery()) {
                while (instance_rs.next()) {
                    int instance_id = instance_rs.getInt(1);
                    /*verify all chains for this instance */
                    verifyInstance(file, filepath, schema_name_int, table_name_int, instance_id, hashColumnData, columnData, globalStats, COPY_BYTESFILE_FOR_FAILED);
                }

                System.out.println("Verified a total of " + globalStats.success_count + " rows");
                if (globalStats.fail_count > 0) {
                    System.out.println("Failed to verify a total of " + globalStats.fail_count + " rows");
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    /**
     * Verify all rows in a single instance.
     *
     * @param file - File to write temp bytes
     * @param filepath - File path
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param hashColumnData - Hash Column Data and previous hash information.
     * @param globalStats - Global success and fail count
     * @param COPY_BYTESFILE_FOR_FAILED - Copy bytes file if hash verification
     * failed.
     */
    private static void verifyInstance(File file, String filepath, String schema_name_int, String table_name_int, int instance_id, HashColumnData hashColumnData, List<ColumnData> columnData, GlobalStats globalStats, boolean COPY_BYTESFILE_FOR_FAILED) {
        Connection con = DBConnection.getInstance().getConnection();
        /* get all chains for this instance */
        String chain_id_qry = "select DISTINCT ORABCTAB_CHAIN_ID$ from " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? ORDER BY ORABCTAB_CHAIN_ID$";
        InstanceStats instanceStats = new InstanceStats();
        try {
            try ( PreparedStatement chain_id_stmt = con.prepareStatement(chain_id_qry)) {
                /* bind instance_id */
                chain_id_stmt.setInt(1, instance_id);
                try ( ResultSet chain_rs = chain_id_stmt.executeQuery()) {
                    while (chain_rs.next()) {
                        int chain_id = chain_rs.getInt(1);
                        /*verify rows for this chain */
                        verifyChain(file, filepath, schema_name_int, table_name_int, instance_id, chain_id, hashColumnData, columnData, instanceStats, COPY_BYTESFILE_FOR_FAILED);
                    }
                    System.out.println("Verified " + instanceStats.success_count + " rows for instance id : " + instance_id);
                    if (instanceStats.fail_count > 0) {
                        System.out.println("Failed to verify " + instanceStats.fail_count + " rows for instance id : " + instance_id);
                    }
                    System.out.println("");
                    if (globalStats != null) {
                        globalStats.success_count += instanceStats.success_count;
                        globalStats.fail_count += instanceStats.fail_count;
                    }
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    /**
     *
     * Verifies all rows in a single chain.
     *
     * @param file - File to write temp bytes
     * @param filepath - File path
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param hashColumnData - Hash Column Data and previous hash information.
     * @param instanceStats - Instance Level Stats
     * @param COPY_BYTESFILE_FOR_FAILED - Copy bytes file if hash verification
     * failed.
     */
    private static void verifyChain(File file, String filepath, String schema_name_int, String table_name_int, int instance_id, int chain_id, HashColumnData hashColumnData, List<ColumnData> columnData, InstanceStats instanceStats, boolean COPY_BYTESFILE_FOR_FAILED) {
        Connection con = DBConnection.getInstance().getConnection();
        /* get all rows for this chain */
        String seq_no_query = "select ORABCTAB_SEQ_NUM$ , ORABCTAB_HASH$ from " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? ORDER BY ORABCTAB_SEQ_NUM$";
        /*verified rows*/
        int verify_count = 0;
        /*verification failed rows */
        int fail_count = 0;
        boolean first_row = true;
        Integer previous_seq = null;
        try {
            try ( PreparedStatement seq_no_stmt = con.prepareStatement(seq_no_query)) {
                /* bind instance_id */
                seq_no_stmt.setInt(1, instance_id);
                /* bind chain_id */
                seq_no_stmt.setInt(2, chain_id);
                try ( ResultSet seq_rs = seq_no_stmt.executeQuery()) {
                    while (seq_rs.next()) {
                        int seq_no = seq_rs.getInt(1);
                        if (!verifySequence(previous_seq, seq_no)) {
                            throw new Error("Invalid Sequence");
                        }
                        if (seq_no == 1) {
                            hashColumnData.prev_hash = null;
                        } else {
                            hashColumnData.prev_hash = hashColumnData.curr_hash;
                        }
                        hashColumnData.curr_hash = seq_rs.getString(2);
                        /* If this is the first row in a chain and the sequence number is not 1 , we assume the row is OK */
                        if (first_row) {
                            first_row = !first_row;
                            if (seq_no != 1) {
                                verify_count++;
                                continue;
                            }
                        }
                        /*verify this row */
                        getBytesForRowHash(file, schema_name_int, table_name_int, instance_id, chain_id, seq_no, hashColumnData, columnData);
                        String calculated_hash = encryptSHA512(filepath);
                        String expected_hash = getRowHash(schema_name_int, table_name_int, instance_id, chain_id, seq_no);
                        if (calculated_hash.equals(expected_hash)) {
                            /*verification successfull */
                            verify_count++;
                        } else {
                            /*verification failed */
                            fail_count++;
                            System.err.println("Hash Verification Failed for instance id : " + instance_id + " , chain id : " + chain_id + " , sequence no : " + seq_no);
                            System.err.println("Expected Hash : " + expected_hash);
                            System.err.println("GOT : " + calculated_hash);
                            if (COPY_BYTESFILE_FOR_FAILED) {
                                /*get parent path */
                                String parent = "BytesFile";
                                String copy_name = parent + instance_id + "_" + chain_id + "_" + seq_no + "." + getFileExtension(file);
                                /*make copy file*/
                                File copy_file = new File(copy_name);
                                try {
                                    /* copy file */
                                    Files.copy(file.toPath(), copy_file.toPath());
                                } catch (IOException e) {
                                    Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
                                }
                            }
                        }
                        previous_seq = seq_no;
                    }
                    System.out.println("Verified " + verify_count + " rows for instance id : " + instance_id + " , chain id : " + chain_id);
                    if (fail_count > 0) {
                        System.err.println("Failed to verify " + fail_count + " rows for instance id : " + instance_id + " , chain id : " + chain_id);
                    }
                    if (instanceStats != null) {
                        instanceStats.success_count += verify_count;
                        instanceStats.fail_count += fail_count;
                    }
                }
            }
        } catch (SQLException e) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, e);
        }

    }

    /**
     * Formulate the row-bytes using instance_id , chain_id , seq_id for a
     * blockchain table t , in schema s.
     *
     * @param file - File to write row bytes
     * @param schema_name_int - User's Schema
     * @param table_name_int - Blockchain Table name
     * @param instance_id - instance id of the row
     * @param chain_id - chain id of the row
     * @param sequence_id - sequence id of the row
     * @param hashColumnData - Hash Column Data and previous hash information.
     */
    private static void getBytesForRowHash(File file, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, HashColumnData hashColumnData, List<ColumnData> columnData) {
        Connection con = DBConnection.getInstance().getConnection();
        /* Column Type */
        String column_type = null;
        /* Column Position */
        Integer col_pos = null;
        /* Certificate ID */
        byte[] cert_id = null;
        /* Certificate ID column position */
        Integer cert_id_pos = null;
        /* Signature Algorithm */
        byte[] sign_algo = null;
        /* Signature Algorithm column position */
        Integer sign_algo_pos = null;
        /* Spare column */
        byte[] spare_col = null;
        try (final OutputStream row_data = new FileOutputStream(file)) {
            for (ColumnData data : columnData) {
                /* fetch column name */
                String column_name = data.column_name;
                /* Enquoted column_name */
                String column_name_quoted = cleanString(column_name, true);
                /* fetch column type */
                column_type = data.column_type;
                /* fetch column position */
                col_pos = data.column_pos;
                /* Block to handle ORABCTAB_SIGNATURE_ALG$ , ORABCTAB_SIGNATURE_CERT$ , ORABCTAB_SPARE$ columns */
                if (column_name.equals("ORABCTAB_SIGNATURE_ALG$") || column_name.equals("ORABCTAB_SIGNATURE_CERT$") || column_name.equals("ORABCTAB_SPARE$")) {
                    /* Query to fetch column data */
                    String col_val_query = "select " + column_name_quoted + " " + "from" + " " + schema_name_int + "."
                            + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and "
                            + "ORABCTAB_SEQ_NUM$ = ?";
                    /* bind instance_id */
                    try ( PreparedStatement col_val_stmt = con.prepareStatement(col_val_query)) {
                        /* bind instance_id */
                        col_val_stmt.setInt(1, instance_id);
                        /* bind chain_id */
                        col_val_stmt.setInt(2, chain_id);
                        /* bind sequence_id */
                        col_val_stmt.setInt(3, sequence_id);
                        /* execute the query */
                        try ( ResultSet col_rs = col_val_stmt.executeQuery()) {
                            while (col_rs.next()) {
                                /* Store signature algorithm and column position */
                                if (column_name.equals("ORABCTAB_SIGNATURE_ALG$")) {
                                    sign_algo_pos = col_pos;
                                    sign_algo = col_rs.getBytes(1);
                                    /* Store certificate id and column position */
                                } else if (column_name.equals("ORABCTAB_SIGNATURE_CERT$")) {
                                    cert_id_pos = col_pos;
                                    cert_id = col_rs.getBytes(1);
                                    /* get spare column value */
                                } else {
                                    spare_col = col_rs.getBytes(1);
                                }
                            }
                        } catch (SQLException ex) {
                            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                            throw new Error();
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
                        throw new Error();
                    }
                    /* Store data in variables if exists and move to the next column */
                    continue;
                }
                /* Handle LOB Data */
                if (column_type.equals("CLOB") || column_type.equals("NCLOB") || column_type.equals("BLOB")) {
                    writeLOBs(con, column_name_quoted, schema_name_int, table_name_int, instance_id, chain_id, sequence_id, column_type, col_pos, row_data);
                    /* Handle Strings */
                } else if (column_type.equals("VARCHAR2") || column_type.equals("CHAR")
                        || column_type.equals("NVARCHAR2") || column_type.equals("NCHAR")) {
                    writeStrings(con, column_name_quoted, schema_name_int, table_name_int, instance_id, chain_id, sequence_id, column_type, col_pos, row_data);
                    /* Handle Other Scalar Types */
                } else {
                    writeScalar(con, column_name_quoted, schema_name_int, table_name_int, instance_id, chain_id, sequence_id, column_type, col_pos, row_data);
                }
            }
            /* If spare column is not null , write spare column data */
            if (spare_col != null) {
                writeSpareColumnData(sign_algo_pos, cert_id_pos, sign_algo, cert_id, spare_col, row_data);
            }
            /* previous row hash */
            byte[] prev_row_hash = null;
            /*  for first entry in a chain(seq_no = 1) generate a byte[] of all 0's as the previous hash */
            if (sequence_id == 1) {
                prev_row_hash = ByteBuffer.allocate(64).putInt(0).array();
            } else {
                prev_row_hash = hashColumnData.prev_hash == null ? RAW.hexString2Bytes(getRowHash(schema_name_int, table_name_int, instance_id, chain_id, sequence_id - 1)) : RAW.hexString2Bytes(hashColumnData.prev_hash);
            }
            /* append metadata to main buffer for all columns */
            row_data.write(populateMetadata(hashColumnData.col_pos, hashColumnData.column_type, 0, 64));
            /* append the previous row has to the main buffer */
            row_data.write(prev_row_hash);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException | SQLException ex) {
            Logger.getLogger(Verify_Rows.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Selects the function to execute depending upon the input supplied based
     * upon instance_id , chain_id and sequence_no.
     *
     * @param filepath - Filepath for writing bytes
     * @param schema - Schema name
     * @param table - Table name
     * @param instance_id - Instance id
     * @param chain_id - Chain id
     * @param sequence_no - Sequence No
     * @param COPY_BYTESFILE_FOR_FAILED - Copy bytes file if hash verification
     * fails failed
     */
    private static void checkVersion(String filepath, String schema, String table, Integer instance_id, Integer chain_id, Integer sequence_no, boolean COPY_BYTESFILE_FOR_FAILED) {
        /* File as specified in file_path */
        File FILE = new File(filepath);
        if (FILE.isDirectory()) {
            System.err.println("Please specify an input file , not directory.");
            System.exit(0);
        }
        boolean success = true;
        String schema_name_int = cleanString(schema, false);
        String table_name_int = cleanString(table, false);
        HashColumnData hashColumnData = getHashColumnData(schema_name_int, table_name_int);
        List<ColumnData> columnData = getColumnData(schema_name_int, table_name_int);
        if (instance_id == null) {
            /* If instance id is not specified , verify all instances */
            verifyAllInstances(FILE, filepath, schema_name_int, table_name_int, hashColumnData, columnData, COPY_BYTESFILE_FOR_FAILED);
        } else if (chain_id == null) {
            /* If chain id is not specified , verify all chains for this instance */
            verifyInstance(FILE, filepath, schema_name_int, table_name_int, instance_id, hashColumnData, columnData, null, COPY_BYTESFILE_FOR_FAILED);
        } else if (sequence_no == null) {
            /*If sequence number is not specified , verify all rows in this chain */
            verifyChain(FILE, filepath, schema_name_int, table_name_int, instance_id, chain_id, hashColumnData, columnData, null, COPY_BYTESFILE_FOR_FAILED);
        } else {
            /* Verify a single row */
            getBytesForRowHash(FILE, schema_name_int, table_name_int, instance_id, chain_id, sequence_no, hashColumnData, columnData);
            String calculated_hash = encryptSHA512(filepath);
            String expected_hash = getRowHash(schema_name_int, table_name_int, instance_id, chain_id, sequence_no);
            if (calculated_hash.equals(expected_hash)) {
                System.out.println("Hash Verification Successful!");
                System.out.println("Hash : " + calculated_hash);
            } else {
                success = false;
                System.err.println("Hash Verfication Failed");
                System.err.println("Expected Hash : " + expected_hash);
                System.err.println("GOT : " + calculated_hash);
            }
        }
        /* Delete temp bytes file created */
        if (!(COPY_BYTESFILE_FOR_FAILED && !success)) {
            if (FILE.delete()) {
                System.out.println("Deleted the file : " + FILE.getName());
            } else {
                System.err.println("Failed to delete the file as the file was not created.");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 6) {
            System.err.println("Invalid Number Of Arguments Supplied.");
            System.err.println("Java program should be run using :");
            System.err.println("java Verify_Rows <COPY_BYTESFILE_FOR_FAILED> <SCHEMA> <TABLE> <INSTANCE_ID - OPTIONAL> <CHAIN_ID - OPTIONAL> <SEQUENCE_NO - OPTIONAL> ");
            System.exit(0);
        }
        try {
            final String FILEPATH = "BytesFile.dat";
            final boolean COPY_BYTESFILE_FOR_FAILED = Boolean.parseBoolean(args[0]);
            final String SCHEMA = args[1];
            final String TABLE = args[2];
            final Integer INSTANCE_ID = args.length >= 4 ? Integer.parseInt(args[3]) : null;
            final Integer CHAIN_ID = args.length >= 5 ? Integer.parseInt(args[4]) : null;
            final Integer SEQUENCE_NO = args.length == 6 ? Integer.parseInt(args[5]) : null;
            checkVersion(FILEPATH, SCHEMA, TABLE, INSTANCE_ID, CHAIN_ID, SEQUENCE_NO, COPY_BYTESFILE_FOR_FAILED);
        } catch (NumberFormatException ex) {
            System.err.println("INSTANCE_ID , CHAIN_ID , SEQUENCE_NO MUST BE AN INTEGER");
            System.exit(0);
        } finally {
            DBConnection.getInstance().closeConnection();
        }
    }
}
