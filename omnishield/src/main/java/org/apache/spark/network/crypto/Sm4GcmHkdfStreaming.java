package org.apache.spark.network.crypto;

import com.google.crypto.tink.subtle.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.util.Arrays;

public class Sm4GcmHkdfStreaming {

    public static final String TRANSFORMATION = "SM4/GCM/NOPadding";
    private final int keySizeInBytes;
    private final int ciphertextSegmentSize;
    private final int plaintextSegmentSize;
    private final int firstSegmentOffset;
    private final String hkdfAlg;
    private final byte[] ikm;

    public Sm4GcmHkdfStreaming(byte[] ikm, String hkdfAlg, int keySizeInBytes, int ciphertextSegmentSize, int firstSegmentOffset) throws IOException {
        if (ikm.length >= 16 && ikm.length >= keySizeInBytes) {
//            Validators.validateAesKeySize(keySizeInBytes);
            if (ciphertextSegmentSize <= firstSegmentOffset + this.getHeaderLength() + 16) {
                // TODO exception类型
                throw new IOException("ciphertextSegmentSize too small");
            } else {
                this.ikm = Arrays.copyOf(ikm, ikm.length);
                this.hkdfAlg = hkdfAlg;
                this.keySizeInBytes = keySizeInBytes;
                this.ciphertextSegmentSize = ciphertextSegmentSize;
                this.firstSegmentOffset = firstSegmentOffset;
                this.plaintextSegmentSize = ciphertextSegmentSize - 16;
            }
        } else {
            throw new IOException("ikm too short, must be >= " + Math.max(16, keySizeInBytes));
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
        return 1 + this.keySizeInBytes + 7;
    }

    public int getCiphertextOffset() {
        return this.getHeaderLength() + this.firstSegmentOffset;
    }

    public int getCiphertextOverhead() {
        return 16;
    }

    public int getFirstSegmentOffset() {
        return this.firstSegmentOffset;
    }

    public long expectedCiphertextSize(long plaintextSize) {
        long offset = (long)this.getCiphertextOffset();
        long fullSegments = (plaintextSize + offset) / (long)this.plaintextSegmentSize;
        long ciphertextSize = fullSegments * (long)this.ciphertextSegmentSize;
        long lastSegmentSize = (plaintextSize + offset) % (long)this.plaintextSegmentSize;
        if (lastSegmentSize > 0L) {
            ciphertextSize += lastSegmentSize + 16L;
        }

        return ciphertextSize;
    }

    private static Cipher cipherInstance() throws GeneralSecurityException {
        return  Cipher.getInstance(TRANSFORMATION, BouncyCastleProvider.PROVIDER_NAME);
    }

    private byte[] randomSalt() {
        return Random.randBytes(this.keySizeInBytes);
    }

    private static GCMParameterSpec paramsForSegment(byte[] prefix, long segmentNr, boolean last) throws GeneralSecurityException {
        ByteBuffer nonce = ByteBuffer.allocate(16);
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
        byte[] key = Hkdf.computeHkdf(this.hkdfAlg, this.ikm, salt, aad, this.keySizeInBytes);
        return new SecretKeySpec(key, "SM4");
    }

    class Sm4GcmHkdfStreamDecrypter implements StreamSegmentDecrypter {
        private SecretKeySpec keySpec;
        private Cipher cipher;
        private byte[] noncePrefix;

        Sm4GcmHkdfStreamDecrypter() {
        }

        public synchronized void init(ByteBuffer header, byte[] aad) throws GeneralSecurityException {
            if (header.remaining() != Sm4GcmHkdfStreaming.this.getHeaderLength()) {
                throw new InvalidAlgorithmParameterException("Invalid header length");
            } else {
                byte firstByte = header.get();
                if (firstByte != Sm4GcmHkdfStreaming.this.getHeaderLength()) {
                    throw new GeneralSecurityException("Invalid ciphertext");
                } else {
                    this.noncePrefix = new byte[7];
                    byte[] salt = new byte[Sm4GcmHkdfStreaming.this.keySizeInBytes];
                    header.get(salt);
                    header.get(this.noncePrefix);
                    this.keySpec = Sm4GcmHkdfStreaming.this.deriveKeySpec(salt, aad);
                    this.cipher = Sm4GcmHkdfStreaming.cipherInstance();
                }
            }
        }

        public synchronized void decryptSegment(ByteBuffer ciphertext, int segmentNr, boolean isLastSegment, ByteBuffer plaintext) throws GeneralSecurityException {
            GCMParameterSpec params = Sm4GcmHkdfStreaming.paramsForSegment(this.noncePrefix, (long)segmentNr, isLastSegment);
            this.cipher.init(2, this.keySpec, params);
            this.cipher.doFinal(ciphertext, plaintext);
        }
    }

    class Sm4GcmHkdfStreamEncrypter implements StreamSegmentEncrypter {
        private final SecretKeySpec keySpec;
        private final Cipher cipher = Sm4GcmHkdfStreaming.cipherInstance();
        private final byte[] noncePrefix;
        private final ByteBuffer header;
        private long encryptedSegments = 0L;

        public Sm4GcmHkdfStreamEncrypter(byte[] aad) throws GeneralSecurityException {
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

        public ByteBuffer getHeader() {
            return this.header.asReadOnlyBuffer();
        }

        public synchronized void encryptSegment(ByteBuffer plaintext, boolean isLastSegment, ByteBuffer ciphertext) throws GeneralSecurityException {
            this.cipher.init(1, this.keySpec, Sm4GcmHkdfStreaming.paramsForSegment(this.noncePrefix, this.encryptedSegments, isLastSegment));
            ++this.encryptedSegments;
            this.cipher.doFinal(plaintext, ciphertext);
        }

        public synchronized void encryptSegment(ByteBuffer part1, ByteBuffer part2, boolean isLastSegment, ByteBuffer ciphertext) throws GeneralSecurityException {
            this.cipher.init(1, this.keySpec, Sm4GcmHkdfStreaming.paramsForSegment(this.noncePrefix, this.encryptedSegments, isLastSegment));
            ++this.encryptedSegments;
            if (part2.hasRemaining()) {
                this.cipher.update(part1, ciphertext);
                this.cipher.doFinal(part2, ciphertext);
            } else {
                this.cipher.doFinal(part1, ciphertext);
            }

        }
    }
}
