/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.File;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;


public class IO {

    private File bytesFile = null;
    private Clob clob;
    private NClob nClob;
    private Blob blob;
    private static IO instance;

    public static IO getIOInstance() {
        if (instance == null) {
            instance = new IO();
        }
        return instance;
    }

    public File getBytesFile() {
        return bytesFile;
    }

    public void setBytesFile(String filepath) {
        if (bytesFile == null) {
            File FILE = new File(Utils.getUtils().cleanPath(filepath));
            if (FILE.isDirectory()) {
                throw new Error("Please specify an input file , not directory.");
            }
            this.bytesFile = FILE;
        }
    }

    public Clob getClob() {
        return clob;
    }

    public void setClob(Clob clob) {
        this.clob = clob;
    }

    public NClob getNClob() {
        return nClob;
    }

    public void setNClob(NClob nClob) {
        this.nClob = nClob;
    }

    public Blob getBlob() {
        return blob;
    }

    public void setBlob(Blob blob) {
        this.blob = blob;
    }
}
