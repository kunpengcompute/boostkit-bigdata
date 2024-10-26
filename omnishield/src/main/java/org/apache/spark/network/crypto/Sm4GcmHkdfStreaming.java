/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.network.crypto;

import com.google.crypto.tink.subtle.*;
import com.huawei.boostkit.omnishield.cipher.SM4Cipher;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.util.Arrays;

/**
 * Sm4GcmHkdfStreaming use the SM4/GCM/NOPadding algorithm to encrypt
 */
public class Sm4GcmHkdfStreaming {
    private static final Logger LOG = LoggerFactory.getLogger(SM4Cipher.class);

    static {
        LOG.info("Shuffle network IO encryption will use SM4/GCM/NOPadding algorithm");
    }

    /**
     * transformation
     */
    public static final String TRANSFORMATION = "SM4/GCM/NOPadding";

    /**
     * sm4 key length
     */
    public static final int KEY_LENGTH = 16;

    /**
     * sm4 gcm block size
     */
    public static final int GCM_BLOCK_SIZE = 16;

    /**
     * sm4 algorithm
     */
    public static final String ALGORITHM = "SM4";
    private final int ciphertextSegmentSize;
    private final int plaintextSegmentSize;
    private final String hkdfAlg;
    private final byte[] keyBytes;


    public Sm4GcmHkdfStreaming(byte[] keyBytes, String hkdfAlg, int ciphertextSegmentSize) throws IOException {
        if (keyBytes.length != KEY_LENGTH) {
            throw new IOException("key length is not equal to " + KEY_LENGTH);
        }
        if (ciphertextSegmentSize <= this.getHeaderLength() + KEY_LENGTH) {
            throw new IOException("ciphertextSegmentSize too small");
        } else {
            this.keyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
            this.hkdfAlg = hkdfAlg;
            this.ciphertextSegmentSize = ciphertextSegmentSize;
            this.plaintextSegmentSize = ciphertextSegmentSize - GCM_BLOCK_SIZE;
        }
    }

    public Sm4GcmHkdfStreaming.Sm4GcmHkdfStreamEncrypter newStreamSegmentEncrypter(byte[] aad) throws GeneralSecurityException {
        return new Sm4GcmHkdfStreaming.Sm4GcmHkdfStreamEncrypter(aad);
    }

    public Sm4GcmHkdfStreaming.Sm4GcmHkdfStreamDecrypter newStreamSegmentDecrypter() throws IOException {
        return new Sm4GcmHkdfStreaming.Sm4GcmHkdfStreamDecrypter();
    }

    public int getPlaintextSegmentSize() {
        return this.plaintextSegmentSize;
    }

    public int getCiphertextSegmentSize() {
        return this.ciphertextSegmentSize;
    }

    public int getHeaderLength() {
        return 1 + KEY_LENGTH + 7;
    }

    public long expectedCiphertextSize(long plaintextSize) {
        long offset = 0;
        long fullSegments = (plaintextSize + offset) / (long)this.plaintextSegmentSize;
        long ciphertextSize = fullSegments * (long)this.ciphertextSegmentSize;
        long lastSegmentSize = (plaintextSize + offset) % (long)this.plaintextSegmentSize;
        if (lastSegmentSize > 0L) {
            ciphertextSize += lastSegmentSize + GCM_BLOCK_SIZE;
        }

        return ciphertextSize + getHeaderLength();
    }

    private static Cipher cipherInstance() throws GeneralSecurityException {
        return  Cipher.getInstance(TRANSFORMATION, BouncyCastleProvider.PROVIDER_NAME);
    }

    private byte[] randomSalt() {
        return Random.randBytes(KEY_LENGTH);
    }

    private static GCMParameterSpec paramsForSegment(byte[] prefix, long segmentNr, boolean last) throws GeneralSecurityException {
        ByteBuffer nonce = ByteBuffer.allocate(12);
        nonce.order(ByteOrder.BIG_ENDIAN);
        nonce.put(prefix);
        SubtleUtil.putAsUnsigedInt(nonce, segmentNr);
        nonce.put((byte)(last ? 1 : 0));
        return new GCMParameterSpec(128, nonce.array());
    }

    private static byte[] randomNonce() {
        return Random.randBytes(7);
    }

    private SecretKeySpec deriveKeySpec(byte[] salt, byte[] aad) throws GeneralSecurityException {
        byte[] key = Hkdf.computeHkdf(this.hkdfAlg, this.keyBytes, salt, aad, KEY_LENGTH);
        return new SecretKeySpec(key, ALGORITHM);
    }

    class Sm4GcmHkdfStreamDecrypter implements GcmCrypto {
        private SecretKeySpec keySpec;
        private Cipher cipher;
        private byte[] noncePrefix;

        Sm4GcmHkdfStreamDecrypter() {
        }

        @Override
        public synchronized void init(ByteBuffer header, byte[] aad) throws GeneralSecurityException {
            if (header.remaining() != Sm4GcmHkdfStreaming.this.getHeaderLength()) {
                throw new InvalidAlgorithmParameterException("Invalid header length");
            } else {
                byte firstByte = header.get();
                if (firstByte != Sm4GcmHkdfStreaming.this.getHeaderLength()) {
                    throw new GeneralSecurityException("Invalid ciphertext");
                } else {
                    this.noncePrefix = new byte[7];
                    byte[] salt = new byte[KEY_LENGTH];
                    header.get(salt);
                    header.get(this.noncePrefix);
                    this.keySpec = Sm4GcmHkdfStreaming.this.deriveKeySpec(salt, aad);
                    this.cipher = Sm4GcmHkdfStreaming.cipherInstance();
                }
            }
        }

        @Override
        public synchronized void decryptSegment(ByteBuffer ciphertext, int segmentNr, boolean isLastSegment, ByteBuffer plaintext) throws GeneralSecurityException {
            GCMParameterSpec params = Sm4GcmHkdfStreaming.paramsForSegment(this.noncePrefix, segmentNr, isLastSegment);
            this.cipher.init(Cipher.DECRYPT_MODE, this.keySpec, params);
            this.cipher.doFinal(ciphertext, plaintext);
        }
    }

    interface GcmCrypto {

        default void init(ByteBuffer header, byte[] aad) throws GeneralSecurityException {
            throw new UnsupportedOperationException("Unsupported function called.");
        }

        default ByteBuffer getHeader(){
            throw new UnsupportedOperationException("Unsupported function called.");
        }

        default void encryptSegment(ByteBuffer plaintext, boolean isLastSegment, ByteBuffer ciphertext) throws GeneralSecurityException {
            throw new UnsupportedOperationException("Unsupported function called.");
        }

        default void decryptSegment(ByteBuffer ciphertext, int segmentNr, boolean isLastSegment, ByteBuffer plaintext) throws GeneralSecurityException {
            throw new UnsupportedOperationException("Unsupported function called.");
        }


    }
    class Sm4GcmHkdfStreamEncrypter implements GcmCrypto {
        private final SecretKeySpec keySpec;
        private final Cipher cipher;
        private final byte[] noncePrefix;
        private final ByteBuffer header;
        private long encryptedSegments;

        public Sm4GcmHkdfStreamEncrypter(byte[] aad) throws GeneralSecurityException {
            this.cipher = Sm4GcmHkdfStreaming.cipherInstance();
            this.encryptedSegments = 0L;
            byte[] salt = Sm4GcmHkdfStreaming.this.randomSalt();
            this.noncePrefix = Sm4GcmHkdfStreaming.randomNonce();
            this.header = ByteBuffer.allocate(Sm4GcmHkdfStreaming.this.getHeaderLength());
            this.header.put((byte)Sm4GcmHkdfStreaming.this.getHeaderLength());
            this.header.put(salt);
            this.header.put(this.noncePrefix);
            this.header.flip();
            this.keySpec = Sm4GcmHkdfStreaming.this.deriveKeySpec(salt, aad);
        }

        @Override
        public ByteBuffer getHeader() {
            return this.header.asReadOnlyBuffer();
        }

        @Override
        public synchronized void encryptSegment(ByteBuffer plaintext, boolean isLastSegment, ByteBuffer ciphertext) throws GeneralSecurityException {
            this.cipher.init(Cipher.ENCRYPT_MODE, this.keySpec, Sm4GcmHkdfStreaming.paramsForSegment(this.noncePrefix, this.encryptedSegments, isLastSegment));
            ++this.encryptedSegments;
            this.cipher.doFinal(plaintext, ciphertext);
        }
    }
}
