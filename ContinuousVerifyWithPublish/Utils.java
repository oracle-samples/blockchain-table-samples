/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Utils {

    private static Utils instance;
    private static final Logger logger = Logger.getLogger(Utils.class.getName());

    public static Utils getUtils() {
        if (instance == null) {
            instance = new Utils();
        }
        return instance;
    }

    /**
     * Cleans the user input by removing unnecessary spaces and normalizing the
     * input for the database if not already enquoted.This method is called only
     * for user inputs such as table name or schema name.If the table name or
     * schema name is already enquoted return that string otherwise sanitize the
     * input and return enquoted literal.
     *
     * @param input - Input String
     * @param alwaysQuoted - Always enquote input or not
     * @return Cleaned String
     */
    public String cleanString(String input, boolean alwaysQuoted) {
        Statement st = null;
        String enquoted = null;
        try {
            Connection con = DBConnection.getInstance().getConnection();
            st = con.createStatement();
            enquoted = st.enquoteIdentifier(input, alwaysQuoted);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
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
    public String unEnquoted(String s) {
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
    public String getFileExtension(File file) {
        String name = file.getName();
        if (name.lastIndexOf(".") != -1 & name.lastIndexOf(".") != 0) {
            return name.substring(name.lastIndexOf(".") + 1);
        } else {
            return "";
        }
    }

    /**
     * Clean the file path
     * @param filename - Name of the file.
     * @return clean filename
     */
    public String cleanPath(String filename) {
        if (filename == null) {
            return null;
        }
        String cleanString = "";
        for (int i = 0; i < filename.length(); ++i) {
            cleanString += cleanCharPath(filename.charAt(i));
        }
        return cleanString;
    }

    private static char cleanCharPath(char aChar) {
        // 0 - 9
        for (int i = 48; i < 58; ++i) {
            if (aChar == i) {
                return (char) i;
            }
        }
        // 'A' - 'Z'
        for (int i = 65; i < 91; ++i) {
            if (aChar == i) {
                return (char) i;
            }
        }
        // 'a' - 'z'
        for (int i = 97; i < 123; ++i) {
            if (aChar == i) {
                return (char) i;
            }
        }
        // other valid characters
        switch (aChar) {
            case '/':
                return '/';
            case '.':
                return '.';
            case '-':
                return '-';
            case '_':
                return '_';
            case ' ':
                return ' ';
        }
        return '%';
    }
}
