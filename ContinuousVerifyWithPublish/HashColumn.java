/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Class to store Hash Column Information and maintain previous row hash for
 * faster verification.
 */
public class HashColumn {

    final private String column_type;
    final private Integer column_position;
    private String prev_hash;
    private String curr_hash;
    private boolean prevRowExists;
    private static HashColumn instance;
    private static final Logger logger = Logger.getLogger(HashColumn.class.getName());

    private HashColumn(String column_type, Integer column_position) {
        this.column_type = column_type;
        this.column_position = column_position;
        this.prev_hash = null;
        this.curr_hash = null;
    }

    /**
     * Stores the hash column data type and position for faster verification.
     *
     * @param schema_name_int - Schema name
     * @param table_name_int - Table name
     */
    public static void initHashColumn(String schema_name_int, String table_name_int) {
        if (instance == null) {
            Connection con = DBConnection.getInstance().getConnection();
            boolean null_check = true;
            String prev_row_hash_metadata_qry = "select data_type, internal_column_id from SYS.ALL_TAB_COLS "
                    + "where OWNER = ? and TABLE_NAME = ? and " + "COLUMN_NAME = 'ORABCTAB_HASH$'";
            try ( PreparedStatement prev_row_hash_metadata_stmt = con.prepareStatement(prev_row_hash_metadata_qry)) {
                /* bind schema_name */
                prev_row_hash_metadata_stmt.setString(1, Utils.getUtils().unEnquoted(schema_name_int));
                /* bind table_name */
                prev_row_hash_metadata_stmt.setString(2, Utils.getUtils().unEnquoted(table_name_int));
                /* execute the query */
                try ( ResultSet rs = prev_row_hash_metadata_stmt.executeQuery()) {
                    if (rs.next()) {
                        null_check = !null_check;
                        instance = new HashColumn(rs.getString(1), rs.getInt(2));
                    }
                    if (null_check) {
                        throw new Error("Invalid Table or Schema name");
                    }
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
    }

    public static HashColumn getHashColumnInstance() {
        return instance;
    }

    public String getColumn_type() {
        return column_type;
    }

    public Integer getColumn_position() {
        return column_position;
    }

    public String getPrev_hash() {
        return prev_hash;
    }

    public void setPrev_hash(String prev_hash) {
        this.prev_hash = prev_hash;
    }

    public String getCurr_hash(String schema_name_int, String table_name_int, int instance_id, int chain_id,
            int sequence_id) {
        if (curr_hash != null) {
            prevRowExists = true;
            return curr_hash;
        } else {
            String previous_hash = getRowHash(schema_name_int, table_name_int, instance_id, chain_id, sequence_id - 1);
            prevRowExists = previous_hash != null;
            return previous_hash;
        }
    }

    public void setCurr_hash(String curr_hash) {
        this.curr_hash = curr_hash;
    }

    public boolean isPrevRowExists() {
        return prevRowExists;
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
    public String getRowHash(String schema_name_int, String table_name_int, int instance_id, int chain_id,
            int sequence_id) {
        Connection con = DBConnection.getInstance().getConnection();
        String current_row_hash = null;
        /* SQL query to fetch the current row hash */
        String current_hash_qry = "SELECT ORABCTAB_HASH$ from " + schema_name_int + "." + table_name_int + " "
                + "where ORABCTAB_INST_ID$ = ? and " + "ORABCTAB_CHAIN_ID$ = ? and " + "ORABCTAB_SEQ_NUM$ = ?";
        try {
            /* bind instance_id */
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
            logger.log(Level.SEVERE, null, e);
        }
        /* return the current row hash */
        return current_row_hash;
    }
}
