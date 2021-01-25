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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.RemotingServer;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Request;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableCollection;
import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;
import static org.apache.dubbo.remoting.Constants.HEARTBEAT_CHECK_TICK;
import static org.apache.dubbo.remoting.Constants.LEAST_HEARTBEAT_DURATION;
import static org.apache.dubbo.remoting.Constants.TICKS_PER_WHEEL;
import static org.apache.dubbo.remoting.utils.UrlUtils.getHeartbeat;
import static org.apache.dubbo.remoting.utils.UrlUtils.getIdleTimeout;

/**
 * ExchangeServerImpl
 * 该类实现了ExchangeServer接口，是基于协议头的信息交换服务器实现类，
 * HeaderExchangeServer是Server的装饰器，每个实现方法都会调用server的方法。
 */
public class HeaderExchangeServer implements ExchangeServer {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    //服务器
    private final RemotingServer server;
    //信息交换服务器是否关闭
    private AtomicBoolean closed = new AtomicBoolean(false);
    //定时任务
    private static final HashedWheelTimer IDLE_CHECK_TIMER = new HashedWheelTimer(new NamedThreadFactory("dubbo-server-idleCheck", true), 1,
            TimeUnit.SECONDS, TICKS_PER_WHEEL);
    //关闭定时任务   provider（服务端）断开链接，处理措施：关闭通道的操作。
    private CloseTimerTask closeTimerTask;

    public HeaderExchangeServer(RemotingServer server) {
        Assert.notNull(server, "server == null");
        this.server = server;
        startIdleCheckTask(getUrl());
    }

    public RemotingServer getServer() {
        return server;
    }

    @Override
    public boolean isClosed() {
        return server.isClosed();
    }

    /**
     * 该方法是检测服务器是否还运行，只要有一个客户端连接着，就算服务器运行着。
     * @return
     */
    private boolean isRunning() {
        Collection<Channel> channels = getChannels();
        // 遍历所有连接该服务器的通道
        for (Channel channel : channels) {

            /**
             *  If there are any client connections,
             *  our server should be running.
             */

            // 只要有任何一个客户端连接，则服务器还运行着
            if (channel.isConnected()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        // 关闭定时任务和心跳检测
        doClose();
        // 关闭线程池和心跳检测
        server.close();
    }

    /**
     * 该close方法是优雅的关闭，有一定的延时来让一些响应或者操作做完。
     * 关闭分两个步骤，第一个就是关闭信息交换服务器中的线程池和心跳检测，然后才是关闭服务器。
     * @param timeout
     */
    @Override
    public void close(final int timeout) {
        // 开始关闭
        startClose();
        if (timeout > 0) {
            final long max = (long) timeout;
            final long start = System.currentTimeMillis();
            if (getUrl().getParameter(Constants.CHANNEL_SEND_READONLYEVENT_KEY, true)) {
                // 发送 READONLY_EVENT事件给所有连接该服务器的客户端，表示 Server 不可读了。
                sendChannelReadOnlyEvent();
            }
            // 当服务器还在运行，并且没有超时，睡眠，也就是等待timeout左右时间在进行关闭
            while (HeaderExchangeServer.this.isRunning()
                    && System.currentTimeMillis() - start < max) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
        // 关闭线程池和心跳检测
        doClose();
        // 延迟关闭
        server.close(timeout);
    }

    @Override
    public void startClose() {
        server.startClose();
    }

    /**
     * 在关闭服务器中有一个操作就是发送事件READONLY_EVENT，
     * 告诉客户端该服务器不可读了，就是该方法实现的，逐个通知连接的客户端该事件
     */
    private void sendChannelReadOnlyEvent() {
        // 创建一个READONLY_EVENT事件的请求
        Request request = new Request();
        request.setEvent(READONLY_EVENT);
        // 不需要响应
        request.setTwoWay(false);
        // 设置版本
        request.setVersion(Version.getProtocolVersion());

        Collection<Channel> channels = getChannels();
        // 遍历连接的通道，进行通知
        for (Channel channel : channels) {
            try {
                // 通过通道还连接着，则发送通知
                if (channel.isConnected()) {
                    channel.send(request, getUrl().getParameter(Constants.CHANNEL_READONLYEVENT_SENT_KEY, true));
                }
            } catch (RemotingException e) {
                logger.warn("send cannot write message error.", e);
            }
        }
    }

    private void doClose() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // 停止心跳检测
        cancelCloseTask();
    }

    private void cancelCloseTask() {
        if (closeTimerTask != null) {
            closeTimerTask.cancel();
        }
    }

    /**
     * 该方法是返回连接该服务器信息交换通道集合。逻辑就是先获得通道集合，
     * 在根据通道来创建信息交换通道，然后返回信息通道集合。
     * @return
     */
    @Override
    public Collection<ExchangeChannel> getExchangeChannels() {
        Collection<ExchangeChannel> exchangeChannels = new ArrayList<ExchangeChannel>();
        // 获得连接该服务器通道集合
        Collection<Channel> channels = server.getChannels();
        if (CollectionUtils.isNotEmpty(channels)) {
            // 遍历通道集合，为每个通道都创建信息交换通道，并且加入信息交换通道集合
            for (Channel channel : channels) {
                exchangeChannels.add(HeaderExchangeChannel.getOrAddChannel(channel));
            }
        }
        return exchangeChannels;
    }

    @Override
    public ExchangeChannel getExchangeChannel(InetSocketAddress remoteAddress) {
        Channel channel = server.getChannel(remoteAddress);
        return HeaderExchangeChannel.getOrAddChannel(channel);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Collection<Channel> getChannels() {
        return (Collection) getExchangeChannels();
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return getExchangeChannel(remoteAddress);
    }

    @Override
    public boolean isBound() {
        return server.isBound();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return server.getLocalAddress();
    }

    @Override
    public URL getUrl() {
        return server.getUrl();
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return server.getChannelHandler();
    }

    @Override
    public void reset(URL url) {
        // 重置属性
        server.reset(url);
        try {
            // 当前重置心跳检查时间
            int currHeartbeat = getHeartbeat(getUrl());
            //当前设置心跳检查超时时间
            int currIdleTimeout = getIdleTimeout(getUrl());
            //最新设置的心跳检查时间
            int heartbeat = getHeartbeat(url);
            //最新设置心跳检查超时时间
            int idleTimeout = getIdleTimeout(url);
            if (currHeartbeat != heartbeat || currIdleTimeout != idleTimeout) {
                cancelCloseTask();
                // 重新开始心跳
                startIdleCheckTask(url);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void send(Object message) throws RemotingException {
        if (closed.get()) {
            throw new RemotingException(this.getLocalAddress(), null, "Failed to send message " + message
                    + ", cause: The server " + getLocalAddress() + " is closed!");
        }
        server.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        if (closed.get()) {
            throw new RemotingException(this.getLocalAddress(), null, "Failed to send message " + message
                    + ", cause: The server " + getLocalAddress() + " is closed!");
        }
        server.send(message, sent);
    }

    /**
     * Each interval cannot be less than 1000ms.
     */
    private long calculateLeastDuration(int time) {
        if (time / HEARTBEAT_CHECK_TICK <= 0) {
            return LEAST_HEARTBEAT_DURATION;
        } else {
            return time / HEARTBEAT_CHECK_TICK;
        }
    }

    private void startIdleCheckTask(URL url) {
        if (!server.canHandleIdle()) {
            AbstractTimerTask.ChannelProvider cp = () -> unmodifiableCollection(HeaderExchangeServer.this.getChannels());
            //设置超时时间默认为心跳检查的3倍
            int idleTimeout = getIdleTimeout(url);
            //定时任务触发时间
            long idleTimeoutTick = calculateLeastDuration(idleTimeout);
            CloseTimerTask closeTimerTask = new CloseTimerTask(cp, idleTimeoutTick, idleTimeout);
            this.closeTimerTask = closeTimerTask;

            // 开始心跳
            // init task and start timer.
            IDLE_CHECK_TIMER.newTimeout(closeTimerTask, idleTimeoutTick, TimeUnit.MILLISECONDS);
        }
    }
}
