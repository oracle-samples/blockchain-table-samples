/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */


public class Modes {

    private static Modes instance;
    /* Copy the Bytesfile if hash verification failed ? */
    private boolean COPY_BYTESFILE_FOR_FAILED;
    /* Is the program running in continuous verification mode ? */
    private int CONTINUOUS_VERIFICATION_MODE;
    /* Is the metadata of the program stored locally or on OBP ?  */
    private int METADATA_STORAGE_MODE;

    public static Modes getInstance() {
        if (instance == null) {
            instance = new Modes();
        }
        return instance;
    }

    public boolean isCOPY_BYTESFILE_FOR_FAILED() {
        return COPY_BYTESFILE_FOR_FAILED;
    }

    public void setCOPY_BYTESFILE_FOR_FAILED(boolean COPY_BYTESFILE_FOR_FAILED) {
        this.COPY_BYTESFILE_FOR_FAILED = COPY_BYTESFILE_FOR_FAILED;
    }

    public int getCONTINUOUS_VERIFICATION_MODE() {
        return CONTINUOUS_VERIFICATION_MODE;
    }

    public void setCONTINUOUS_VERIFICATION_MODE(int CONTINUOUS_VERIFICATION_MODE) {
        this.CONTINUOUS_VERIFICATION_MODE = CONTINUOUS_VERIFICATION_MODE;
    }

    public int getMETADATA_STORAGE_MODE() {
        return METADATA_STORAGE_MODE;
    }

    public void setMETADATA_STORAGE_MODE(int METADATA_STORAGE_MODE) {
        this.METADATA_STORAGE_MODE = METADATA_STORAGE_MODE;
    }
}
