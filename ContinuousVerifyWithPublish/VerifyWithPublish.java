/* 
 * ContinuousVerifyWithPublish Version 1.0
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
import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.sql.CharacterSet;
import oracle.sql.RAW;


public class VerifyWithPublish {
    
    private static final Logger logger = Logger.getLogger(VerifyWithPublish.class.getName());
    
    /**
     * Writes the spare column data if not null.
     *
     * @param sign_algo_pos - Signature algorithm column position
     * @param cert_id_pos - Certificate id column position
     * @param sign_algo - Signature Algorithm
     * @param cert_id - Certificate ID
     * @param spare_col - Spare column data
     */
    static void writeSpareColumnData(Integer sign_algo_pos, Integer cert_id_pos, byte[] sign_algo, byte[] cert_id, byte[] spare_col) {
        /* Get spare column as int value */
        Integer spare_int = ByteBuffer.wrap(spare_col).order(ByteOrder.LITTLE_ENDIAN).getInt();
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true)) {
            /* ORABCTAB_SIGNATURE_ALG$ has been set */
            if ((spare_int & 1) == 1) {
                byteStream.write(populateMetadata(sign_algo_pos, Constants.DB_NUMBER, 0, sign_algo.length));
                byteStream.write(sign_algo);
            }
            /* ORABCTAB_SIGNATURE_CERT$ has been set */
            if ((spare_int & 2) == 2) {
                byteStream.write(populateMetadata(cert_id_pos, Constants.DB_RAW, 0, cert_id.length));
                byteStream.write(cert_id);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     * Writes LOBS(CLOB , NCLOB , BLOB) type columns to disk
     *
     * @param column_name_quoted - Column name
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param instance_id - Instance id
     * @param chain_id - Chain id
     * @param sequence_id - Sequence number
     * @param column_type - Column type
     * @param col_pos - Column position
     */
    static void writeLOBs(String column_name_quoted, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, String column_type, int col_pos) {
        /* length of the column - 8 BYTE VALUE */
        long column_length = 0;
        /* is the column null? - 1 BYTE VALUE */
        int column_isnull = 0;
        /* Query to fetch column data */
        String col_val_query = "select " + column_name_quoted + " " + "from" + " " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and "
                + "ORABCTAB_SEQ_NUM$ = ?";
        try ( PreparedStatement col_val_stmt = DBConnection.getInstance().getConnection().prepareStatement(col_val_query)) {
            /* bind instance_id */
            col_val_stmt.setInt(1, instance_id);
            /* bind chain_id */
            col_val_stmt.setInt(2, chain_id);
            /* bind sequence_id */
            col_val_stmt.setInt(3, sequence_id);
            /* execute the query */
            try ( ResultSet col_rs = col_val_stmt.executeQuery()) {
                if (col_rs.next()) {
                    switch (column_type) {
                        case Constants.DB_CLOB:
                            {
                                /* fetch the data inside java.sql.Clob */
                                Clob temp_val = col_rs.getClob(1);
                                /* column length */
                                column_length = temp_val == null ? 0 : temp_val.length() * 2;
                                /* check for null value of Clob */
                                if (temp_val == null) {
                                    column_isnull = 1;
                                } else {
                                    /* set the Character Stream for the Clob Value */
                                    IO.getIOInstance().setClob(temp_val);
                                }       break;
                            }
                        case Constants.DB_NCLOB:
                            {
                                /* fetch the data inside java.sql.NClob */
                                NClob temp_val = col_rs.getNClob(1);
                                /* column length */
                                column_length = temp_val == null ? 0 : temp_val.length() * 2;
                                /* check for null value of NClob */
                                if (temp_val == null) {
                                    column_isnull = 1;
                                } else {
                                    /* set the Character Stream for the NClob Value */
                                    IO.getIOInstance().setNClob(temp_val);
                                }       break;
                            }
                        case Constants.DB_BLOB:
                            {
                                /* fetch the data inside java.sql.Blob */
                                Blob temp_val = col_rs.getBlob(1);
                                /* column length */
                                column_length = temp_val == null ? 0 : temp_val.length();
                                /* check for null value of Blob */
                                if (temp_val == null) {
                                    column_isnull = 1;
                                } else {
                                    /* set the Byte Stream for the Blob Value */
                                    IO.getIOInstance().setBlob(temp_val);
                                }       break;
                            }
                        default:
                            break;
                    }
                }
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true)) {
            /* append metadata to main buffer for all columns */
            byteStream.write(populateMetadata(col_pos, column_type, column_isnull, column_length));
            /* append column value if not null */
            if (column_length != 0) {
                if (column_type.equals(Constants.DB_BLOB)) {
                    /* Read BLOB using InputStream , then write to disk */
                    DBUtils.getDBUtils().writeBlob();
                } else if (column_type.equals(Constants.DB_CLOB)) {
                    /* Read CLOB character by character and get AL16UTF16 byte representation , then write to disk*/
                    DBUtils.getDBUtils().writeClob();
                } else {
                    /* Read NCLOB character by character and get AL16UTF16 byte representation , then write to disk*/
                    DBUtils.getDBUtils().writeNClob();
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * Writes Strings(VARCHAR2 , NVARCHAR2 , CHAR , NCHAR) type columns to disk.
     *
     * @param column_name_quoted - Column name
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param instance_id - instance id
     * @param chain_id - chain id
     * @param sequence_id - sequence number
     * @param column_type - column type
     * @param col_pos - column position
     */
    static void writeStrings(String column_name_quoted, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, String column_type, int col_pos) {
        /* is the column null? - 1 BYTE VALUE */
        int column_isnull = 0;
        /* temporary buffer for column value */
        byte[] temp_bytes = null;
        /* Query to fetch column data */
        String col_val_query = "select " + column_name_quoted + " " + "from" + " " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and "
                + "ORABCTAB_SEQ_NUM$ = ?";
        try ( PreparedStatement col_val_stmt = DBConnection.getInstance().getConnection().prepareStatement(col_val_query)) {
            /* bind instance_id */
            col_val_stmt.setInt(1, instance_id);
            /* bind chain_id */
            col_val_stmt.setInt(2, chain_id);
            /* bind sequence_id */
            col_val_stmt.setInt(3, sequence_id);
            /* execute the query */
            try ( ResultSet col_rs = col_val_stmt.executeQuery()) {
                if (col_rs.next()) {
                    /* fetch the data inside String */
                    String temp_val = (column_type.equals(Constants.DB_VARCHAR) || column_type.equals(Constants.DB_CHAR))
                            ? col_rs.getString(1)
                            : col_rs.getNString(1);
                    /* check for null value of String */
                    if (temp_val == null) {
                        column_isnull = 1;
                    } else {
                        if (column_type.equals(Constants.DB_CHAR) || column_type.equals(Constants.DB_NCHAR)) {
                            /* trim blanks except for one blank in an all-blank value */
                            temp_val = temp_val.trim();
                            if (temp_val.length() == 0) {
                                temp_val = " ";
                            }
                        }
                        if (column_type.equals(Constants.DB_VARCHAR) || column_type.equals(Constants.DB_CHAR)) {
                            /* Normalize: fetch the AL32UTF8 bytes */
                            temp_bytes = CharacterSet.stringToAL32UTF8(temp_val);
                        } else if (column_type.equals(Constants.DB_NVARCHAR) || column_type.equals(Constants.DB_NCHAR)) {
                            /* Normalize: fetch the AL16UTF16 bytes */
                            temp_bytes = CharacterSet.stringToAL16UTF16Bytes(temp_val);
                        }
                    }
                }
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true)) {
            /* length of the column - 8 BYTE VALUE */
            long column_length = temp_bytes == null ? 0 : temp_bytes.length;
            /* append metadata to main buffer for all columns */
            byteStream.write(populateMetadata(col_pos, column_type, column_isnull, column_length));
            if (column_length != 0) {
                /* Write to Disk */
                byteStream.write(temp_bytes);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     * Writes Other Scalar type columns to disk.
     *
     * @param column_name_quoted - Column name
     * @param schema_name_int - Schema Name
     * @param table_name_int - Table Name
     * @param instance_id - Instance Id
     * @param chain_id - Chain Id
     * @param sequence_id - Sequence Number
     * @param column_type - Column Type
     * @param col_pos - Column Position
     */
    static void writeScalar(Connection con, String column_name_quoted, String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id, String column_type, int col_pos) {
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
            /* bind instance_id */
            try ( PreparedStatement col_val_stmt = con.prepareStatement(col_val_query)) {
                /* bind instance_id */
                col_val_stmt.setInt(1, instance_id);
                /* bind chain_id */
                col_val_stmt.setInt(2, chain_id);
                /* bind sequence_id */
                col_val_stmt.setInt(3, sequence_id);
                /* execute the query */
                try ( ResultSet col_rs = col_val_stmt.executeQuery(); 
                      final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true)) {
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
                    byteStream.write(populateMetadata(col_pos, column_type, column_isnull,
                            column_length));
                    if (column_length != 0) {
                        /* Write to Disk */
                        byteStream.write(temp_bytes);
                    }
                }
            }
        } catch (SQLException | IOException ex) {
            logger.log(Level.SEVERE, null, ex);
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
        int column_type_int = DBUtils.getDBUtils().getDataTypeID(column_type);
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
            logger.log(Level.SEVERE, null, e);
        } finally {
            try {
                temp_metadata.close();
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        return temp_metadata.toByteArray();
    }

    /**
     * Hash the row-bytes got using getBytesForRowHash() using SHA-512
     * scheme.
     *
     * @param file_path - Path to row bytes file
     * @return - SHA-512 Hashed Row Bytes
     */
    private static String hashSHA512(String file_path) {
        String ht = null;
        InputStream is = null;
        try {
            /* getInstance() called with SHA-512 Algorithm */
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            /* calculate the message digest after getting row-bytes */
            is = new FileInputStream(Utils.getUtils().cleanPath(file_path));
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
            logger.log(Level.SEVERE, null, e);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
        }
        /* convert to oracle.sql.RAW and return the SHA-512 hash */
        return DBUtils.getDBUtils().RAWToString(ht);
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
     * @param filepath - File path
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     */
    private static void verifyAllInstances(String filepath, String schema_name_int, String table_name_int) {
        /* Get the connection */
        Connection con = DBConnection.getInstance().getConnection();
        /* get all instances */
        String instance_id_qry = "select DISTINCT ORABCTAB_INST_ID$ from " + schema_name_int + "." + table_name_int + " ORDER BY ORABCTAB_INST_ID$";
        /* Prepare to create global level stats */
        Stats globalStats = new Stats();
        try {
            try ( PreparedStatement instance_id_stmt = con.prepareStatement(instance_id_qry);  ResultSet instance_rs = instance_id_stmt.executeQuery()) {
                while (instance_rs.next()) {
                    int instance_id = instance_rs.getInt(1);
                    /*verify all chains for this instance */
                    verifyInstance(filepath, schema_name_int, table_name_int, instance_id, globalStats);
                }

                System.out.println("Verified a total of " + globalStats.getSuccess_count() + " rows");
                if (globalStats.getFail_count() > 0) {
                    System.out.println("Failed to verify a total of " + globalStats.getFail_count() + " rows");
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     * Verify all rows in a single instance.
     *
     * @param filepath - File path
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param globalStats - Global success and fail count
     */
    private static void verifyInstance(String filepath, String schema_name_int, String table_name_int, int instance_id, Stats globalStats) {
        /* Get connection */
        Connection con = DBConnection.getInstance().getConnection();
        /* get all chains for this instance */
        String chain_id_qry = "select DISTINCT ORABCTAB_CHAIN_ID$ from " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? ORDER BY ORABCTAB_CHAIN_ID$";
        /* Prepare to create instance - level stats */
        Stats instanceStats = new Stats();
        try {
            try ( PreparedStatement chain_id_stmt = con.prepareStatement(chain_id_qry)) {
                /* bind instance_id */
                chain_id_stmt.setInt(1, instance_id);
                try ( ResultSet chain_rs = chain_id_stmt.executeQuery()) {
                    while (chain_rs.next()) {
                        int chain_id = chain_rs.getInt(1);
                        /*verify rows for this chain */
                        verifyChain(filepath, schema_name_int, table_name_int, instance_id, chain_id, instanceStats);
                    }
                    System.out.println("Verified " + instanceStats.getSuccess_count() + " rows for instance id : " + instance_id);
                    if (instanceStats.getFail_count() > 0) {
                        System.out.println("Failed to verify " + instanceStats.getFail_count() + " rows for instance id : " + instance_id);
                    }
                    System.out.println("");
                    if (globalStats != null) {
                        globalStats.addSuccess(instanceStats.getSuccess_count());
                        globalStats.addFailure(instanceStats.getFail_count());
                    }
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     *
     * Verifies all rows in a single chain.
     *
     * @param filepath - File path
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     * @param instanceStats - Instance Level Stats
     * failed.
     */
    private static void verifyChain(String filepath, String schema_name_int, String table_name_int, int instance_id, int chain_id, Stats instanceStats) {
        /* Get connection */
        Connection con = DBConnection.getInstance().getConnection();
        /* get all rows for this chain */
        /* Get all rows with sequence number greater than the current checkpoint because we are in
         * in a continous verification mode and we don't watch to verify previously verified rows
         * again and again.
         */
        String seq_no_query = "select ORABCTAB_SEQ_NUM$ , ORABCTAB_HASH$ from " + schema_name_int + "."
                + table_name_int + " " + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? "
                + "AND ORABCTAB_SEQ_NUM$ > ? ORDER BY ORABCTAB_SEQ_NUM$";
        /*verified rows*/
        int verify_count = 0;
        /*verification failed rows */
        int fail_count = 0;
        Integer previous_seq = null;
        try {
            try ( PreparedStatement seq_no_stmt = con.prepareStatement(seq_no_query)) {
                /* bind instance_id */
                seq_no_stmt.setInt(1, instance_id);
                /* bind chain_id */
                seq_no_stmt.setInt(2, chain_id);
                /* get checkpoint data */
                seq_no_stmt.setInt(3, CheckPoint.getInstance().getSequenceValue(instance_id, chain_id));
                try ( ResultSet seq_rs = seq_no_stmt.executeQuery()) {
                    Integer seq_no = null;
                    while (seq_rs.next()) {
                        seq_no = seq_rs.getInt(1);
                        /* Sequence validation error */
                        if (!verifySequence(previous_seq, seq_no)) {
                            throw new Error("Invalid Sequence");
                        }
                        if (seq_no == 1) {
                            HashColumn.getHashColumnInstance().setPrev_hash(null);
                        } else {
                            HashColumn.getHashColumnInstance().setPrev_hash(HashColumn.getHashColumnInstance().getCurr_hash(schema_name_int, table_name_int, instance_id, chain_id, seq_no));
                        }
                        HashColumn.getHashColumnInstance().setCurr_hash(seq_rs.getString(2));
                        /* If this is the first row in a chain and the sequence number is not 1 , we assume the row is OK */
                        if (!HashColumn.getHashColumnInstance().isPrevRowExists() && seq_no != 1) {
                            verify_count++;
                            continue;
                        }
                        /*verify this row */
                        getBytesForRowHash(schema_name_int, table_name_int, instance_id, chain_id, seq_no);
                        String calculated_hash = hashSHA512(filepath);
                        String expected_hash = HashColumn.getHashColumnInstance().getRowHash(schema_name_int, table_name_int, instance_id, chain_id, seq_no);
                        if (calculated_hash.equals(expected_hash)) {
                            /*verification successfull */
                            verify_count++;
                            /* If we are in a continous verification mode we need to start building the log */
                            if(Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE() != Constants.MODE_OFF)
                                new LogBuilder(schema_name_int, table_name_int, instance_id, chain_id, seq_no, calculated_hash).publish();
                        } else {
                            /*verification failed */
                            fail_count++;
                            System.err.println("Hash Verification Failed for instance id : " + instance_id + " , chain id : " + chain_id + " , sequence no : " + seq_no);
                            System.err.println("Expected Hash : " + expected_hash);
                            System.err.println("GOT : " + calculated_hash);
                            if (Modes.getInstance().isCOPY_BYTESFILE_FOR_FAILED()) {
                                /*get parent path */
                                String db_guid = DBUtils.getDBUtils().getDbGUID();
                                String parent = schema_name_int + "_" + table_name_int + "_" + db_guid + "_" + "bytesfile";
                                String copy_name = parent + instance_id + "_" + chain_id + "_" + seq_no + "." + Utils.getUtils().getFileExtension(IO.getIOInstance().getBytesFile());
                                /*make copy file*/
                                File copy_file = new File(Utils.getUtils().cleanPath(copy_name));
                                try {
                                    /* copy file */
                                    Files.copy(IO.getIOInstance().getBytesFile().toPath(), copy_file.toPath());
                                } catch (IOException e) {
                                    logger.log(Level.SEVERE, null, e);
                                }
                            }
                            /* If we are in a continous verification mode we need to start building the log */
                            if(Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE() != Constants.MODE_OFF)
                                new LogBuilder(schema_name_int, table_name_int, instance_id, chain_id, seq_no, calculated_hash, expected_hash).publish();
                        }
                        previous_seq = seq_no;
                    }
                    if(seq_no != null) {
                        /* set the last seq_no seen for this chain_id */
                        CheckPoint.getInstance().setLastSeenSequence(instance_id, chain_id, seq_no);
                    }
                    System.out.println("Verified " + verify_count + " rows for instance id : " + instance_id + " , chain id : " + chain_id);
                    if (fail_count > 0) {
                        System.err.println("Failed to verify " + fail_count + " rows for instance id : " + instance_id + " , chain id : " + chain_id);
                    }
                    if (instanceStats != null) {
                        instanceStats.addSuccess(verify_count);
                        instanceStats.addFailure(fail_count);
                    }
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, null, e);
        }

    }

    /**
     * Formulate the row-bytes using instance_id , chain_id , seq_id for a
     * blockchain table t , in schema s.
     *
     * @param schema_name_int - User's Schema
     * @param table_name_int - Blockchain Table name
     * @param instance_id - instance id of the row
     * @param chain_id - chain id of the row
     * @param sequence_id - sequence id of the row
     */
    private static void getBytesForRowHash(String schema_name_int, String table_name_int, int instance_id,
            int chain_id, int sequence_id) {
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
        /* Lets clear all the previous bytes */
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile())) {
            byteStream.close();
        } catch (FileNotFoundException ex) {
            throw new Error("Bytes File Not Found!");
        } catch (IOException ex) {
            throw new Error();
        }
        
        /* Look through all the columns */
        for (ColumnData data : ColumnData.getColumnData()) {
            /* fetch column name */
            String column_name = data.getColumn_name();
            /* Enquoted column_name */
            String column_name_quoted = Utils.getUtils().cleanString(column_name, true);
            /* fetch column type */
            column_type = data.getColumn_type();
            /* fetch column position */
            col_pos = data.getColumn_position();
            /* Block to handle ORABCTAB_SIGNATURE_ALG$ , ORABCTAB_SIGNATURE_CERT$ , ORABCTAB_SPARE$ columns */
            if (column_name.equals(Constants.DB_SIG_ALGO) || column_name.equals(Constants.DB_SIG_CERT) || column_name.equals(Constants.DB_BC_SPARE)) {
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
                            switch (column_name) {
                                /* Store signature algorithm and column position */
                                case Constants.DB_SIG_ALGO:
                                    sign_algo_pos = col_pos;
                                    sign_algo = col_rs.getBytes(1);
                                    break;
                                /* Store certificate id and column position */
                                case Constants.DB_SIG_CERT:
                                    cert_id_pos = col_pos;
                                    cert_id = col_rs.getBytes(1);
                                    break;
                                /* get spare column value */
                                default:
                                    spare_col = col_rs.getBytes(1);
                                    break;
                            }
                        }
                    } catch (SQLException ex) {
                        logger.log(Level.SEVERE, null, ex);
                        throw new Error();
                    }
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, null, ex);
                    throw new Error();
                }
                /* Store data in variables if exists and move to the next column */
                continue;
            }
            switch (column_type) {
                /* Handle LOB Data */
                case Constants.DB_CLOB:
                case Constants.DB_NCLOB:
                case Constants.DB_BLOB:
                    writeLOBs(column_name_quoted, schema_name_int, table_name_int, instance_id, chain_id, sequence_id, column_type, col_pos);
                    break;
                /* Handle Strings */
                case Constants.DB_VARCHAR:
                case Constants.DB_CHAR:
                case Constants.DB_NVARCHAR:
                case Constants.DB_NCHAR:
                    writeStrings(column_name_quoted, schema_name_int, table_name_int, instance_id, chain_id, sequence_id, column_type, col_pos);
                    break;
                /* Handle Other Scalar Types */
                default:
                    writeScalar(con, column_name_quoted, schema_name_int, table_name_int, instance_id, chain_id, sequence_id, column_type, col_pos);
                    break;
            }
        }
        /* If spare column is not null , write spare column data */
        if (spare_col != null) {
            writeSpareColumnData(sign_algo_pos, cert_id_pos, sign_algo, cert_id, spare_col);
        }
        try (final OutputStream byteStream = new FileOutputStream(IO.getIOInstance().getBytesFile(), true)) {
            /* previous row hash */
            byte[] prev_row_hash = null;
            /*  for first entry in a chain(seq_no = 1) generate a byte[] of all 0's as the previous hash */
            if (sequence_id == 1) {
                prev_row_hash = ByteBuffer.allocate(64).putInt(0).array();
            } else {
                prev_row_hash = HashColumn.getHashColumnInstance().getPrev_hash() == null ? 
                        RAW.hexString2Bytes(HashColumn.getHashColumnInstance().getRowHash(schema_name_int, table_name_int, instance_id, chain_id, sequence_id - 1)) : 
                        RAW.hexString2Bytes(HashColumn.getHashColumnInstance().getPrev_hash());
            }
            /* append metadata to main buffer for all columns */
            byteStream.write(populateMetadata(HashColumn.getHashColumnInstance().getColumn_position(), HashColumn.getHashColumnInstance().getColumn_type(), 0, 64));
            /* append the previous row hash to the main buffer */
            byteStream.write(prev_row_hash);
        } catch (SQLException | IOException e) {
            logger.log(Level.SEVERE, null, e);
            throw new Error();
        } 
    }

    /**
     * Selects the function to execute depending upon the input supplied based
     * upon instance_id , chain_id and sequence_no.
     *
     * @param schema - Schema name
     * @param table - Table name
     * @param instance_id - Instance id
     * @param chain_id - Chain id
     * @param sequence_no - Sequence No
     * @param COPY_BYTESFILE_FOR_FAILED - Copy bytes file if hash verification
     * fails failed
     */
    private static void checkVersion(String schema, String table, Integer instance_id, Integer chain_id, Integer sequence_no) {
        /* Clean schema and table name */
        String schema_name_int = Utils.getUtils().cleanString(schema, false);
        String table_name_int = Utils.getUtils().cleanString(table, false);
        String db_guid = DBUtils.getDBUtils().getDbGUID();
        final String filepath = schema_name_int + "_" + table_name_int + "_" + db_guid + "_" + "bytesfile" +".dat";
        /* File as specified in file_path */
        IO.getIOInstance().setBytesFile(filepath);
        /* Initialization */
        HashColumn.initHashColumn(schema_name_int, table_name_int);
        ColumnData.initColumnData(schema_name_int, table_name_int);
        /* Get the current mode */
        int mode = Modes.getInstance().getCONTINUOUS_VERIFICATION_MODE();
        /* Load upto the point we had previously verified */
        if(mode != Constants.MODE_OFF) {
            CheckPoint.getInstance().initCheckPoints(schema_name_int, table_name_int);
        }
        boolean success = true;
        if (instance_id == null) {
            /* If instance id is not specified , verify all instances */
            verifyAllInstances(filepath, schema_name_int, table_name_int);
        } else if (chain_id == null) {
            /* If chain id is not specified , verify all chains for this instance */
            verifyInstance(filepath, schema_name_int, table_name_int, instance_id, null);
        } else if (sequence_no == null) {
            /*If sequence number is not specified , verify all rows in this chain */
            verifyChain(filepath, schema_name_int, table_name_int, instance_id, chain_id, null);
        } else {
            /* Verify a single row */
            getBytesForRowHash(schema_name_int, table_name_int, instance_id, chain_id, sequence_no);
            String calculated_hash = hashSHA512(filepath);
            String expected_hash = HashColumn.getHashColumnInstance().getRowHash(schema_name_int, table_name_int, instance_id, chain_id, sequence_no);
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
        /* Make a savepoint */
        if(mode != Constants.MODE_OFF) {
            CheckPoint.getInstance().exportCheckPoints(schema_name_int, table_name_int);
        }
        /* If we are building the log locally check if it exceeded 1GB size. If yes copy that
           with timestamp and start a fresh log.
        */
        if(mode == Constants.MODE_LOCAL) {
            String fname = schema_name_int + "_" + table_name_int + "_" + db_guid + ".log";
            File file = new File(Utils.getUtils().cleanPath(fname));
            /* If file is greater than a GB */
            if(file.exists() && (file.length() / (1024 * 1024)) > 1024) {
                try {
                    String copykey = schema_name_int + "_" + table_name_int + "_" + "_" + db_guid + "_" + new Timestamp(System.currentTimeMillis()).getTime() + ".log";
                    File copyFile = new File(Utils.getUtils().cleanPath(copykey));
                    Files.copy(file.toPath(), copyFile.toPath());
                    file.delete();
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            }
        }
        /* Delete temp bytes file created */
        if (!(Modes.getInstance().isCOPY_BYTESFILE_FOR_FAILED() && !success)) {
            if (IO.getIOInstance().getBytesFile().delete()) {
                System.out.println("Deleted the file : " + IO.getIOInstance().getBytesFile().getName());
            } else {
                System.err.println("Failed to delete the file as the file was not created.");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 4 || args.length > 7) {
            System.err.println("Invalid Number Of Arguments Supplied.");
            System.err.println("Java program should be run using :");
            System.err.println("java Verify_Rows1 <COPY_BYTESFILE_FOR_FAILED> <CONTINOUS_VERIFICATION_MODE> <SCHEMA> "
                    + "<TABLE> <INSTANCE_ID - OPTIONAL> <CHAIN_ID - OPTIONAL> <SEQUENCE_NO - OPTIONAL> ");
            throw new Error("Invalid Number Of Arguments Supplied.");
        }
        try {
            final boolean COPY_BYTESFILE_FOR_FAILED = Boolean.parseBoolean(args[0]);
            final int CONTINUOUS_VERIFICATION_MODE = Integer.parseInt(args[1]);
            final String SCHEMA = args[2];
            final String TABLE = args[3];
            final Integer INSTANCE_ID = args.length >= 5 ? Integer.parseInt(args[4]) : null;
            final Integer CHAIN_ID = args.length >= 6 ? Integer.parseInt(args[5]) : null;
            final Integer SEQUENCE_NO = args.length == 7 ? Integer.parseInt(args[6]) : null;
            
            /* Set BytesFile for Failed mode and Continous Verification Mode */
            Modes.getInstance().setCOPY_BYTESFILE_FOR_FAILED(COPY_BYTESFILE_FOR_FAILED);
            Modes.getInstance().setCONTINUOUS_VERIFICATION_MODE(CONTINUOUS_VERIFICATION_MODE);
            /* Verifier for continous or single time verification */
            ScheduledExecutorService verifier = Executors.newScheduledThreadPool(0);
            /* Task to be executed continously or one time */
            Runnable task = () -> {
                checkVersion(SCHEMA, TABLE, INSTANCE_ID, CHAIN_ID, SEQUENCE_NO);
            };
            /* Let's run it for a single time first. First time execution may take some time because we do some environment setup as well also a user
               can have a large blockchain table initially which needs to be parsed before invoking continous verification */
            ScheduledFuture<?> single = verifier.schedule(task, 0, TimeUnit.SECONDS);
            try {
                single.get();
            } catch (InterruptedException | ExecutionException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            
            /* Now check if we have continous verification enabled as well, then we setup for 5 minutes */
            if(CONTINUOUS_VERIFICATION_MODE > Constants.MODE_OFF) {
                if(INSTANCE_ID != null && CHAIN_ID != null && SEQUENCE_NO != null) {
                    System.err.println("Continous Verification Mode cannot be enabled for a single row verification!");
                    return;
                }
              /* First invocation after 300 seconds and then again repeat in 300 seconds */
              ScheduledFuture<?> continous = verifier.scheduleAtFixedRate(task, 5 * 60, 5 * 60, TimeUnit.SECONDS);  
            }
        } catch (NumberFormatException ex) {
            System.err.println("CONTINUOUS_VERIFICATION_MODE, INSTANCE_ID , CHAIN_ID OR SEQUENCE_NO MUST BE AN INTEGER");
        }
    }
}
