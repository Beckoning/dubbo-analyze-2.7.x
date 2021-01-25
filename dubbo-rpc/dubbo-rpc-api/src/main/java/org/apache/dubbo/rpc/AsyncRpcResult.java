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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.dubbo.common.utils.ReflectUtils.defaultReturn;

/**
 * This class represents an unfinished RPC call, it will hold some context information for this call, for example RpcContext and Invocation,
 * so that when the call finishes and the result returns, it can guarantee all the contexts being recovered as the same as when the call was made
 * before any callback is invoked.
 * <p>
 * TODO if it's reasonable or even right to keep a reference to Invocation?
 * <p>
 * As {@link Result} implements CompletionStage, {@link AsyncRpcResult} allows you to easily build a async filter chain whose status will be
 * driven entirely by the state of the underlying RPC call.
 * <p>
 * AsyncRpcResult does not contain any concrete value (except the underlying value bring by CompletableFuture), consider it as a status transfer node.
 * {@link #getValue()} and {@link #getException()} are all inherited from {@link Result} interface, implementing them are mainly
 * for compatibility consideration. Because many legacy {@link Filter} implementation are most possibly to call getValue directly.
 */
public class AsyncRpcResult implements Result {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcResult.class);

    /**
     * RpcContext may already have been changed when callback happens, it happens when the same thread is used to execute another RPC call.
     * So we should keep the reference of current RpcContext instance and restore it before callback being executed.
     */
    /**
     * 当相同的线程用于执行另一个RPC调用时，并且回调发生时，原来的RpcContext可能已经被更改。
     * 所以我们应该保留当前RpcContext实例的引用，并在执行回调之前恢复它。
     * 存储当前的RpcContext
     */
    private RpcContext storedContext;
    /**
     * 存储当前的ServerContext
     */
    private RpcContext storedServerContext;
    private Executor executor;
    /**
     * 会话域
     */
    private Invocation invocation;

    private CompletableFuture<AppResponse> responseFuture;

    /**
     * 上面的是AsyncRpcResult核心的变量以及构造函数，storedContext和storedServerContext存储了相关的RpcContext实例的引用，
     * 为的就是防止在回调的时候由于相同的线程用于执行另一个RPC调用导致原来的RpcContext可能已经被更改。
     * 所以存储下来后，我们需要在执行回调之前恢复它。具体的可以看下面的whenCompleteWithContext方法。
     * @param future
     * @param invocation
     */
    public AsyncRpcResult(CompletableFuture<AppResponse> future, Invocation invocation) {
        this.responseFuture = future;
        // 设置会话域
        this.invocation = invocation;
        // 获得当前线程内代表消费者端的Context
        this.storedContext = RpcContext.getContext();
        // 获得当前线程内代表服务端的Context
        this.storedServerContext = RpcContext.getServerContext();
    }

    /**
     * Notice the return type of {@link #getValue} is the actual type of the RPC method, not {@link AppResponse}
     *
     * @return
     */
    @Override
    public Object getValue() {
        return getAppResponse().getValue();
    }

    /**
     * CompletableFuture can only be completed once, so try to update the result of one completed CompletableFuture will
     * has no effect. To avoid this problem, we check the complete status of this future before update it's value.
     *
     * But notice that trying to give an uncompleted CompletableFuture a new specified value may face a race condition,
     * because the background thread watching the real result will also change the status of this CompletableFuture.
     * The result is you may lose the value you expected to set.
     *
     * @param value
     */
    @Override
    public void setValue(Object value) {
        try {
            if (responseFuture.isDone()) {
                responseFuture.get().setValue(value);
            } else {
                // 创建一个AppResponse实例
                AppResponse appResponse = new AppResponse();
                // 把结果放入AppResponse
                appResponse.setValue(value);
                // 标志该future完成，并且把携带结果的appResponse设置为该future的结果
                responseFuture.complete(appResponse);
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }
    }

    @Override
    public Throwable getException() {
        // 获得抛出的异常信息
        return getAppResponse().getException();
    }

    @Override
    public void setException(Throwable t) {
        try {
            if (responseFuture.isDone()) {
                responseFuture.get().setException(t);
            } else {
                // 创建一个AppResponse实例
                AppResponse appResponse = new AppResponse();
                // 把异常放入appResponse
                appResponse.setException(t);
                // 标志该future完成，并且把携带异常的appResponse设置为该future的结果
                responseFuture.complete(appResponse);
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }
    }

    @Override
    public boolean hasException() {
        // 设置是否有抛出异常
        return getAppResponse().hasException();
    }

    public CompletableFuture<AppResponse> getResponseFuture() {
        return responseFuture;
    }

    public void setResponseFuture(CompletableFuture<AppResponse> responseFuture) {
        this.responseFuture = responseFuture;
    }

    public Result getAppResponse() {
        // 如果该结果计算完成，则直接调用get方法获得结果
        try {
            if (responseFuture.isDone()) {
                return responseFuture.get();
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }

        // 创建AppResponse
        return createDefaultValue(invocation);
    }

    /**
     * This method will always return after a maximum 'timeout' waiting:
     * 1. if value returns before timeout, return normally.
     * 2. if no value returns after timeout, throw TimeoutException.
     *
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public Result get() throws InterruptedException, ExecutionException {
        if (executor != null && executor instanceof ThreadlessExecutor) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();
        }
        return responseFuture.get();
    }

    @Override
    public Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (executor != null && executor instanceof ThreadlessExecutor) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();
        }
        return responseFuture.get(timeout, unit);
    }

    @Override
    public Object recreate() throws Throwable {
        // 强制类型转化
        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        // 如果返回的是future类型
        if (InvokeMode.FUTURE == rpcInvocation.getInvokeMode()) {
            return RpcContext.getContext().getFuture();
        }

        return getAppResponse().recreate();
    }

    public Result whenCompleteWithContext(BiConsumer<Result, Throwable> fn) {
        this.responseFuture = this.responseFuture.whenComplete((v, t) -> {
            beforeContext.accept(v, t);
            fn.accept(v, t);
            afterContext.accept(v, t);
        });
        return this;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn) {
        return this.responseFuture.thenApply(fn);
    }

    @Override
    @Deprecated
    public Map<String, String> getAttachments() {
        return getAppResponse().getAttachments();
    }

    @Override
    public Map<String, Object> getObjectAttachments() {
        return getAppResponse().getObjectAttachments();
    }

    @Override
    public void setAttachments(Map<String, String> map) {
        getAppResponse().setAttachments(map);
    }

    @Override
    public void setObjectAttachments(Map<String, Object> map) {
        getAppResponse().setObjectAttachments(map);
    }

    @Deprecated
    @Override
    public void addAttachments(Map<String, String> map) {
        getAppResponse().addAttachments(map);
    }

    @Override
    public void addObjectAttachments(Map<String, Object> map) {
        getAppResponse().addObjectAttachments(map);
    }

    @Override
    public String getAttachment(String key) {
        return getAppResponse().getAttachment(key);
    }

    @Override
    public Object getObjectAttachment(String key) {
        return getAppResponse().getObjectAttachment(key);
    }

    @Override
    public String getAttachment(String key, String defaultValue) {
        return getAppResponse().getAttachment(key, defaultValue);
    }

    @Override
    public Object getObjectAttachment(String key, Object defaultValue) {
        return getAppResponse().getObjectAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, String value) {
        setObjectAttachment(key, value);
    }

    @Override
    public void setAttachment(String key, Object value) {
        setObjectAttachment(key, value);
    }

    @Override
    public void setObjectAttachment(String key, Object value) {
        getAppResponse().setAttachment(key, value);
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }


    /**
     * tmp context to use when the thread switch to Dubbo thread.
     * 临时的RpcContext，当用户线程切换为Dubbo线程时候使用
     */
    /**
     * 临时的RpcContext
     */
    private RpcContext tmpContext;

    private RpcContext tmpServerContext;
    private BiConsumer<Result, Throwable> beforeContext = (appResponse, t) -> {
        // 获得当前线程消费者端的RpcContext
        tmpContext = RpcContext.getContext();
        // 获得当前线程服务端的RpcContext
        tmpServerContext = RpcContext.getServerContext();
        // 重新设置消费者端的RpcContext
        RpcContext.restoreContext(storedContext);
        // 重新设置服务端的RpcContext
        RpcContext.restoreServerContext(storedServerContext);
    };

    private BiConsumer<Result, Throwable> afterContext = (appResponse, t) -> {
        // 重新把临时的RpcContext设置回去
        RpcContext.restoreContext(tmpContext);
        RpcContext.restoreServerContext(tmpServerContext);
    };

    /**
     * Some utility methods used to quickly generate default AsyncRpcResult instance.
     */
    public static AsyncRpcResult newDefaultAsyncResult(AppResponse appResponse, Invocation invocation) {
        return new AsyncRpcResult(CompletableFuture.completedFuture(appResponse), invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Invocation invocation) {
        return newDefaultAsyncResult(null, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Invocation invocation) {
        return newDefaultAsyncResult(value, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Throwable t, Invocation invocation) {
        return newDefaultAsyncResult(null, t, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Throwable t, Invocation invocation) {
        CompletableFuture<AppResponse> future = new CompletableFuture<>();
        AppResponse result = new AppResponse();
        if (t != null) {
            result.setException(t);
        } else {
            result.setValue(value);
        }
        future.complete(result);
        return new AsyncRpcResult(future, invocation);
    }

    private static Result createDefaultValue(Invocation invocation) {
        ConsumerMethodModel method = (ConsumerMethodModel) invocation.get(Constants.METHOD_MODEL);
        return method != null ? new AppResponse(defaultReturn(method.getReturnClass())) : new AppResponse();
    }
}
