/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson , Stephane Giron

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.read.ErrorPacket;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Options;

import javax.crypto.Cipher;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;

import static org.mariadb.jdbc.internal.com.Packet.EOF;
import static org.mariadb.jdbc.internal.com.Packet.ERROR;

public class SendSha256PasswordAuthPacket extends AbstractAuthSwitchSendResponsePacket implements InterfaceAuthSwitchSendResponsePacket {

    private Options options;
    private PacketInputStream reader;

    /**
     * Constructor of SendSha256PasswordAuthPacket.
     *
     * @param password                  password
     * @param authData                  authData
     * @param packSeq                   packet sequence
     * @param passwordCharacterEncoding password charset
     * @param options                   connection options
     * @param reader                    input stream
     */
    public SendSha256PasswordAuthPacket(String password, byte[] authData, int packSeq, String passwordCharacterEncoding,
                                        Options options, PacketInputStream reader) {
        super(packSeq, authData, password, passwordCharacterEncoding);
        this.options = options;
        this.reader = reader;
    }

    /**
     * Send SHA256 password stream.
     *
     * @param pos database socket
     * @throws IOException if a connection error occur
     */
    public void send(PacketOutputStream pos) throws IOException, SQLException {
        if (password == null || password.equals("")) {
            pos.writeEmptyPacket(reader.getLastPacketSeq() + 1);
            return;
        }

        if (options.useSsl) {
            //can send plain text pwd
            pos.startPacket(reader.getLastPacketSeq() + 1);
            byte[] bytePwd;
            if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
                bytePwd = password.getBytes(passwordCharacterEncoding);
            } else {
                bytePwd = password.getBytes();
            }
            pos.write(bytePwd);
            pos.write(0);
            pos.flush();
        } else {
            PublicKey publicKey;
            if (options.serverRsaPublicKeyFile != null && !options.serverRsaPublicKeyFile.isEmpty()) {
                publicKey = readPublicKeyFromFile(options);
            } else {
                //TODO add allowPublicKeyRetrieval option ?

                //ask public Key Retrieval
                pos.startPacket(reader.getLastPacketSeq() + 1);
                pos.write((byte) 1);
                pos.flush();
                publicKey = readPublicKeyFromSocket(reader);
            }

            sendSha256PasswordPacket(publicKey, password, authData, passwordCharacterEncoding, pos, reader.getLastPacketSeq() + 1);
        }
    }

    /**
     * Send packet with Sha 256 password.
     *
     * @param publicKey                 public key
     * @param password                  password
     * @param seed                      seed
     * @param passwordCharacterEncoding password encoding
     * @param pos                       output stream
     * @param packetSeq                 next packet sequence
     * @throws SQLException                 if cannot encode password
     * @throws UnsupportedEncodingException if password encoding is unknown
     */
    public static void sendSha256PasswordPacket(PublicKey publicKey, String password, byte[] seed,
                                                String passwordCharacterEncoding, PacketOutputStream pos, int packetSeq)
            throws SQLException, UnsupportedEncodingException {
        try {
            byte[] cipherBytes = getCypherBytes(publicKey, password, seed, passwordCharacterEncoding);
            pos.startPacket(packetSeq);
            pos.write(cipherBytes);
            pos.flush();
        } catch (Exception ex) {
            throw new SQLException("Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
        }
    }

    /**
     * Encode password with seed and public key.
     *
     * @param publicKey                 public key
     * @param password                  password
     * @param seed                      seed
     * @param passwordCharacterEncoding password encoding
     * @return encoded password
     * @throws SQLException                 if cannot encode password
     * @throws UnsupportedEncodingException if password encoding is unknown
     */
    public static byte[] getCypherBytes(PublicKey publicKey, String password, byte[] seed, String passwordCharacterEncoding)
            throws SQLException, UnsupportedEncodingException {

        byte[] correctedSeed;
        if (seed.length > 0) {
            //Seed is ended with a null byte value.
            correctedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
        } else {
            correctedSeed = new byte[0];
        }

        byte[] bytePwd;
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
            bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
            bytePwd = password.getBytes();
        }

        byte[] nullFinishedPwd = Arrays.copyOf(bytePwd, bytePwd.length + 1);
        byte[] xorBytes = new byte[nullFinishedPwd.length];
        int seedLength = correctedSeed.length;

        for (int i = 0; i < xorBytes.length; i++) {
            xorBytes[i] = (byte) (nullFinishedPwd[i] ^ correctedSeed[i % seedLength]);
        }

        try {
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher.doFinal(xorBytes);
        } catch (Exception ex) {
            throw new SQLException("Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
        }
    }

    /**
     * Get SHA256 encoded password bytes or [1] to ask for public key.
     *
     * @param options   options
     * @param password  password
     * @param seed      authentication data
     * @param pos       output stream writer
     * @param reader    input stream reader
     * @return password encoded or bytes asking for public key from server.
     * @throws SQLException if certificate error occur
     * @throws IOException  if passwordCharacterEncoding format is unknown
     */
    public static byte[] getSha256pwd(Options options, String password, byte[] seed, PacketOutputStream pos,
                                      PacketInputStream reader) throws SQLException, IOException {
        if (password == null || password.equals("")) {
            return new byte[0];
        }

        if (options.useSsl) {
            //can send plain text pwd
            byte[] pwd;
            if (options.passwordCharacterEncoding != null
                    && !options.passwordCharacterEncoding.isEmpty()) {
                pwd = password.getBytes(options.passwordCharacterEncoding);
            } else {
                pwd = password.getBytes();
            }
            byte[] nullTerminatedPwd = new byte[pwd.length + 1];
            System.arraycopy(pwd, 0, nullTerminatedPwd, 0, pwd.length);
            return nullTerminatedPwd;
        } else {
            PublicKey publicKey;
            if (options.serverRsaPublicKeyFile != null && !options.serverRsaPublicKeyFile.isEmpty()) {
                publicKey = readPublicKeyFromFile(options);
            } else {
                //TODO add allowPublicKeyRetrieval option ?
                return new byte[]{1};
            }

            try {
                return getCypherBytes(publicKey, password, seed, options.passwordCharacterEncoding);
            } catch (Exception ex) {
                throw new SQLException("Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
            }
        }
    }

    /**
     * Read public Key from file.
     *
     * @return public key
     * @throws SQLException if cannot read file or file content is not a public key.
     */
    private static PublicKey readPublicKeyFromFile(Options options) throws SQLException {
        byte[] keyBytes;
        try {
            keyBytes = Files.readAllBytes(Paths.get(options.serverRsaPublicKeyFile));
        } catch (IOException ex) {
            throw new SQLException("Could not read server RSA public key from file : "
                    + "serverRsaPublicKeyFile=" + options.serverRsaPublicKeyFile, "S1009", ex);
        }
        return getPublicKeyFromKey(keyBytes);
    }

    /**
     * Read public Key from socket.
     *
     * @param reader input stream reader
     * @return public key
     * @throws SQLException if server return an Error packet or public key cannot be parsed.
     * @throws IOException  if error reading socket
     */
    public static PublicKey readPublicKeyFromSocket(PacketInputStream reader) throws SQLException, IOException {
        Buffer buffer = reader.getPacket(true);

        if (buffer.getByteAt(0) == ERROR) {
            ErrorPacket ep = new ErrorPacket(buffer);
            String message = ep.getMessage();
            throw new SQLException("Could not connect: " + message, ep.getSqlState(), ep.getErrorNumber());
        }

        if (buffer.getByteAt(0) == EOF) {
            //Erroneous AuthSwitchRequest packet when security exception
            throw new SQLException("Could not connect: receive AuthSwitchRequest in place of RSA public key. "
                    + "Did user has the rights to connect to database ?");
        }

        buffer.skipByte();
        return getPublicKeyFromKey(buffer.readBytesNullEnd());
    }

    /**
     * Read public pem key from String.
     *
     * @param publicKeyBytes public key bytes value
     * @return public key
     * @throws SQLException if key cannot be parsed
     */
    private static PublicKey getPublicKeyFromKey(byte[] publicKeyBytes) throws SQLException {
        try {
            String publicKey = new String(publicKeyBytes).replaceAll("(-+BEGIN PUBLIC KEY-+\\r?\\n|\\n?-+END PUBLIC KEY-+\\r?\\n?)", "");
            byte[] keyBytes = Base64.getMimeDecoder().decode(publicKey);
            X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(spec);
        } catch (Exception ex) {
            throw new SQLException("Could read server RSA public key: " + ex.getMessage(), "S1009", ex);
        }

    }
}
