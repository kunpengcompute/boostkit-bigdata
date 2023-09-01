/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package nova.hetu.olk.memory;

import io.airlift.log.Logger;
import nova.hetu.omniruntime.memory.MemoryManager;

import java.util.concurrent.atomic.AtomicBoolean;

public class OpenLooKengMemoryManager
        extends MemoryManager
{
    private static final Logger log = Logger.get(OpenLooKengMemoryManager.class);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public OpenLooKengMemoryManager()
    {
        super();
    }

    @Override
    protected void finalize() throws Throwable
    {
        try {
            this.close();
        }
        finally {
            super.finalize();
        }
    }

    @Override
    public void close()
    {
        if (isClosed.compareAndSet(false, true)) {
            super.close();
        }
    }
}
