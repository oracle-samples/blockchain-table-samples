/* 
 * PublishHash Version 1.0
 * 
 * Copyright (c) 2021 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 *
 */

import java.io.Console;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;


public class AuthHeader {

    private static AuthHeader instance;
    private byte[] auth_header;

    private AuthHeader() {
        Console credentials = System.console();
        if (credentials == null) {
            throw new Error("Console Not Available");
        }
        System.out.println("Enter Oracle Blockchain Platform Username:");
        final char[] BLOCKCHAIN_PLATFORM_USERNAME = credentials.readLine().toCharArray();
        System.out.println("Enter Oracle Blockchain Platform Password:");
        final char[] BLOCKCHAIN_PLATFORM_PASSWORD = credentials.readPassword();
        final char[] AUTH = getAuth(BLOCKCHAIN_PLATFORM_USERNAME, BLOCKCHAIN_PLATFORM_PASSWORD);
        /* Clear Off what we read */
        Arrays.fill(BLOCKCHAIN_PLATFORM_USERNAME, '\u0000');
        Arrays.fill(BLOCKCHAIN_PLATFORM_PASSWORD, '\u0000');
        /* Get the encoded auth header bytes */
        byte[] encodedAuth = Base64.getEncoder().encode(getAuthBytes(AUTH));
        this.auth_header = encodedAuth;
        Arrays.fill(AUTH, '\u0000');
    }

    public byte[] getAuthHeaderValue() {
        return auth_header;
    }

    public static AuthHeader getInstance() {
        if (instance == null) {
            instance = new AuthHeader();
        }
        return instance;
    }

    private char[] getAuth(char[] username, char[] password) {
        char[] result = new char[username.length + password.length + 1];
        System.arraycopy(username, 0, result, 0, username.length);
        result[username.length] = ':';
        System.arraycopy(password, 0, result, username.length + 1, password.length);
        return result;
    }

    private byte[] getAuthBytes(char[] auth) {
        CharBuffer charBuffer = CharBuffer.wrap(auth);
        ByteBuffer byteBuffer = Charset.forName("UTF-8").encode(charBuffer);
        byte[] authBytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0);
        return authBytes;
    }
}
