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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Class to store column name , column type and column position of each column
 * required to calculate hash.
 */
public class ColumnData {

    final private String column_name;
    final private String column_type;
    final private Integer column_position;
    private static List<ColumnData> column_list;
    private static final Logger logger = Logger.getLogger(ColumnData.class.getName());

    private ColumnData(String column_name, String column_type, Integer column_position) {
        this.column_name = column_name;
        this.column_type = column_type;
        this.column_position = column_position;
    }

    /**
     * Get Column data - Column Name , Column Type & Column Position for all
     * columns required to calculate hash.
     *
     * @param schema_name_int - Schema Name
     * @param table_name_int - Table Name
     */
    public static void initColumnData(String schema_name_int, String table_name_int) {
        if (column_list == null) {
            Connection con = DBConnection.getInstance().getConnection();
            String column_names_query = "select column_name, data_type, internal_column_id from SYS.ALL_TAB_COLS "
                    + "where OWNER = ? and TABLE_NAME = ? and "
                    + "COLUMN_NAME NOT IN ('ORABCTAB_HASH$','ORABCTAB_SIGNATURE$')"
                    + "and VIRTUAL_COLUMN='NO' order by INTERNAL_COLUMN_ID";
            /* Data for all columns */
            column_list = new ArrayList<>();
            try ( PreparedStatement col_names_stmt = con.prepareStatement(column_names_query)) {
                /* bind schema_name */
                col_names_stmt.setString(1, Utils.getUtils().unEnquoted(schema_name_int));
                /* bind table_name */
                col_names_stmt.setString(2, Utils.getUtils().unEnquoted(table_name_int));
                /* execute query */
                try ( ResultSet rs = col_names_stmt.executeQuery()) {
                    while (rs.next()) {
                        /* New column */
                        ColumnData data = new ColumnData(rs.getString(1), rs.getString(2), rs.getInt(3));
                        /* Add to the list */
                        column_list.add(data);
                    }
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
    }

    public static List<ColumnData> getColumnData() {
        return column_list;
    }

    public String getColumn_name() {
        return column_name;
    }

    public String getColumn_type() {
        return column_type;
    }

    public Integer getColumn_position() {
        return column_position;
    }
}
