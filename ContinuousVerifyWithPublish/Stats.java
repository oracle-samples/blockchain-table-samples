/* 
 * ContinuousVerifyWithPublish Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */


/**
 * Maintain Instance & Global Level Stats.
 */
public class Stats {

    private Integer success_count;
    private Integer fail_count;
    private static Stats instance;

    public Stats() {
        success_count = 0;
        fail_count = 0;
    }

    public static Stats getStats() {
        if (instance == null) {
            instance = new Stats();
        }
        return instance;
    }

    public void addSuccess(int count) {
        success_count += count;
    }

    public void addFailure(int count) {
        fail_count += count;
    }

    public Integer getSuccess_count() {
        return success_count;
    }

    public Integer getFail_count() {
        return fail_count;
    }
}
