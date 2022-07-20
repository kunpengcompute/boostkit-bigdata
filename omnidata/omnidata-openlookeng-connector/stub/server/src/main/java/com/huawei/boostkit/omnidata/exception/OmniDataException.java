/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.boostkit.omnidata.exception;

import static com.huawei.boostkit.omnidata.exception.OmniErrorCode.OMNIDATA_GENERIC_ERROR;

/**
 * OmniDataException
 *
 * @since 2022-07-18
 */
public class OmniDataException extends RuntimeException {
    private static final  long serialVersionUID = -9034897193745766939L;

    private final OmniErrorCode errorCode;

    public OmniDataException(String message) {
        super(message);
        errorCode = OMNIDATA_GENERIC_ERROR;
    }

    public OmniDataException(String message, Throwable throwable) {
        super(message, throwable);
        errorCode = OMNIDATA_GENERIC_ERROR;
    }

    public OmniDataException(OmniErrorCode omniErrorCode, String message) {
        super(message);
        errorCode = omniErrorCode;
    }

    public OmniErrorCode getErrorCode() {
        return errorCode;
    }
}
