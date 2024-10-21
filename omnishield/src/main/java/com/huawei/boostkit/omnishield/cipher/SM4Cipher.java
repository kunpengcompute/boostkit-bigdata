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
package com.huawei.boostkit.omnishield.cipher;

import org.apache.commons.crypto.cipher.CryptoCipher;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Properties;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

/**
 * SM4Cipher -- SM4/GCM/NOPadding
 */
public class SM4Cipher implements CryptoCipher {

    private static final Logger LOG = LoggerFactory.getLogger(SM4Cipher.class);

    static {
        LOG.info("Shuffle disk IO encryption will use SM4/GCM/NOPadding algorithm");
    }

    private final Cipher cipher;

    private final String transformation;

    public SM4Cipher(final Properties props, final String transformation) // NOPMD
        throws GeneralSecurityException {
        this.transformation = transformation;
        cipher = Cipher.getInstance(transformation, BouncyCastleProvider.PROVIDER_NAME);
    }

    @Override
    public void updateAAD(byte[] aad) throws IllegalArgumentException, IllegalStateException, UnsupportedOperationException {
        cipher.updateAAD(aad);
    }

    @Override
    public void updateAAD(ByteBuffer aad) throws IllegalArgumentException, IllegalStateException, UnsupportedOperationException {
        cipher.updateAAD(aad);
    }

    @Override
    public int getBlockSize() {
        return cipher.getBlockSize();
    }

    @Override
    public String getAlgorithm() {
        return transformation;
    }

    @Override
    public void init(int mode, Key key, AlgorithmParameterSpec params) throws InvalidKeyException, InvalidAlgorithmParameterException {
        cipher.init(mode, key, params);
    }

    @Override
    public int update(ByteBuffer inBuffer, ByteBuffer outBuffer) throws ShortBufferException {
        return cipher.update(inBuffer, outBuffer);
    }

    @Override
    public int update(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset) throws ShortBufferException {
        return cipher.update(input, inputOffset, inputLen, output, outputOffset);
    }

    @Override
    public int doFinal(ByteBuffer inBuffer, ByteBuffer outBuffer) throws ShortBufferException, IllegalBlockSizeException, BadPaddingException {
        return cipher.doFinal(inBuffer, outBuffer);
    }

    @Override
    public int doFinal(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset) throws ShortBufferException, IllegalBlockSizeException, BadPaddingException {
        return cipher.doFinal(input, inputOffset, inputLen, output, outputOffset);
    }

    @Override
    public void close() throws IOException {
        // do nothing here
    }
}
