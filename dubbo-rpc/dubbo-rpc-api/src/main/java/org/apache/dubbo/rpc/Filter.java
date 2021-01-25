/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.extension.SPI;

/**
 * Extension for intercepting the invocation for both service provider and consumer, furthermore, most of
 * functions in dubbo are implemented base on the same mechanism. Since every time when remote method is
 * invoked, the filter extensions will be executed too, the corresponding penalty should be considered before
 * more filters are added.
 * <pre>
 *  They way filter work from sequence point of view is
 *    <b>
 *    ...code before filter ...
 *          invoker.invoke(invocation) //filter work in a filter implementation class
 *          ...code after filter ...
 *    </b>
 *    Caching is implemented in dubbo using filter approach. If cache is configured for invocation then before
 *    remote call configured caching type's (e.g. Thread Local, JCache etc) implementation invoke method gets called.
 * </pre>
 * Filter. (SPI, Singleton, ThreadSafe)
 *
 * @see org.apache.dubbo.rpc.filter.GenericFilter
 * @see org.apache.dubbo.rpc.filter.EchoFilter
 * @see org.apache.dubbo.rpc.filter.TokenFilter
 * @see org.apache.dubbo.rpc.filter.TpsLimitFilter
 */
//该接口是invoker调用时过滤器接口，其中就只有一个invoke方法。在该方法中对调用进行过滤
@SPI
public interface Filter {
    /**
     * Make sure call invoker.invoke() in your implementation.
     */
    Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException;

    //监听回调

    /**
     * 在Filter接口新增了内部接口Listener，相关接口设计如下。
     *
     * 优点：职责划分更加明确，进行逻辑分组，增强可读性，Filter本身应仅是传递调用的响应，而所有回调都放入Listener。
     *      这样做以后可以把之前回调的逻辑从invoke里面剥离出来，放到Listener的onResponse或者onError中。
     */
    interface Listener {

        void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation);

        void onError(Throwable t, Invoker<?> invoker, Invocation invocation);
    }

}