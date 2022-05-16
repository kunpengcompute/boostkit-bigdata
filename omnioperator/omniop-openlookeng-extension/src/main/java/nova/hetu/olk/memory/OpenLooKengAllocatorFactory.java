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

import nova.hetu.omniruntime.vector.VecAllocator;

import java.util.HashMap;
import java.util.Map;

public class OpenLooKengAllocatorFactory
{
    private static Map<String, VecAllocator> vecAllocators = new HashMap();

    private OpenLooKengAllocatorFactory()
    {
    }

    /**
     * create the vector allocator with specified scope and call back.
     *
     * @param scope scope the specified scope
     * @param createCallBack createCallBack the call back
     * @return vector allocator
     */
    public static synchronized VecAllocator create(String scope, CallBack createCallBack)
    {
        VecAllocator allocator = vecAllocators.get(scope);
        if (allocator == null) {
            allocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR.newChildAllocator(scope, VecAllocator.UNLIMIT, 0);
            vecAllocators.put(scope, new OpenLooKengVecAllocator(allocator.getNativeAllocator()));
            if (createCallBack != null) {
                createCallBack.callBack();
            }
        }
        return allocator;
    }

    /**
     * get the vector allocator by specified scope
     *
     * @param scope scope the scope for vector
     * @return vector allocator
     */
    public static synchronized VecAllocator get(String scope)
    {
        if (vecAllocators.containsKey(scope)) {
            return vecAllocators.get(scope);
        }
        return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
    }

    /**
     * delete the vector allocator by specified scope.
     *
     * @param scope scope the scope for vector
     */
    public static synchronized void delete(String scope)
    {
        VecAllocator allocator = vecAllocators.get(scope);
        if (allocator != null) {
            vecAllocators.remove(scope);
            allocator.close();
        }
    }

    /**
     * remove this allocator from vecAllocators
     *
     * @param scope scope the scope for vector
     * @return removed allocator
     */
    public static synchronized VecAllocator remove(String scope)
    {
        VecAllocator allocator = vecAllocators.get(scope);
        if (allocator != null) {
            vecAllocators.remove(scope);
        }
        return allocator;
    }

    /**
     * the call back interface
     *
     * @since 2022-05-16
     */
    public interface CallBack
    {
        void callBack();
    }
}
