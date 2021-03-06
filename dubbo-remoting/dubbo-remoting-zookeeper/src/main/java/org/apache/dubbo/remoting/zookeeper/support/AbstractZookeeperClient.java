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
package org.apache.dubbo.remoting.zookeeper.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.DataListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

/**
 * 该类实现了ZookeeperClient接口，是客户端的抽象类，
 * 它实现了一些公共逻辑，把具体的doClose、createPersistent等方法抽象出来，留给子类来实现
 * @param <TargetDataListener>
 * @param <TargetChildListener>
 */
public abstract class AbstractZookeeperClient<TargetDataListener, TargetChildListener> implements ZookeeperClient {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractZookeeperClient.class);

    protected int DEFAULT_CONNECTION_TIMEOUT_MS = 5 * 1000;
    protected int DEFAULT_SESSION_TIMEOUT_MS = 60 * 1000;
    /**
     * url对象
     */
    private final URL url;
    /**
     * 状态监听器集合
     */
    private final Set<StateListener> stateListeners = new CopyOnWriteArraySet<StateListener>();
    /**
     * 客户端监听器集合
     */
    private final ConcurrentMap<String, ConcurrentMap<ChildListener, TargetChildListener>> childListeners = new ConcurrentHashMap<String, ConcurrentMap<ChildListener, TargetChildListener>>();

    /**
     * 数据监听--数据发生变化触发监听器
     */
    private final ConcurrentMap<String, ConcurrentMap<DataListener, TargetDataListener>> listeners = new ConcurrentHashMap<String, ConcurrentMap<DataListener, TargetDataListener>>();
    /**
     * 是否关闭
     */
    private volatile boolean closed = false;
    /**
     * 持久存在节点路径
     */
    private final Set<String>  persistentExistNodePath = new ConcurrentHashSet<>();

    public AbstractZookeeperClient(URL url) {
        this.url = url;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public void delete(String path){
        //never mind if ephemeral
        persistentExistNodePath.remove(path);
        deletePath(path);
    }


    /**
     * 该方法是创建客户端的方法，其中createEphemeral和createPersistent方法都被抽象出来，具体实现需要查看子类
     * @param path
     * @param ephemeral
     */
    @Override
    public void create(String path, boolean ephemeral) {
        // 如果不是临时节点
        if (!ephemeral) {
            if(persistentExistNodePath.contains(path)){
                return;
            }
            // 判断该客户端是否存在
            if (checkExists(path)) {
                persistentExistNodePath.add(path);
                return;
            }
        }
        // 获得/的位置
        int i = path.lastIndexOf('/');
        if (i > 0) {
            // 创建客户端
            create(path.substring(0, i), false);
        }
        // 如果是临时节点
        if (ephemeral) {
            // 创建临时节点
            createEphemeral(path);
        } else {
            // 递归创建节点
            createPersistent(path);
            persistentExistNodePath.add(path);
        }
    }

    /**
     * 该方法就是增加状态监听器。
     * @param listener
     */
    @Override
    public void addStateListener(StateListener listener) {
        // 状态监听器加入集合
        stateListeners.add(listener);
    }

    @Override
    public void removeStateListener(StateListener listener) {
        stateListeners.remove(listener);
    }

    public Set<StateListener> getSessionListeners() {
        return stateListeners;
    }

    @Override
    public List<String> addChildListener(String path, final ChildListener listener) {
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.computeIfAbsent(path, k -> new ConcurrentHashMap<>());
        TargetChildListener targetListener = listeners.computeIfAbsent(listener, k -> createTargetChildListener(path, k));
        return addTargetChildListener(path, targetListener);
    }

    @Override
    public void addDataListener(String path, DataListener listener) {
        this.addDataListener(path, listener, null);
    }

    @Override
    public void addDataListener(String path, DataListener listener, Executor executor) {
        ConcurrentMap<DataListener, TargetDataListener> dataListenerMap = listeners.computeIfAbsent(path, k -> new ConcurrentHashMap<>());
        TargetDataListener targetListener = dataListenerMap.computeIfAbsent(listener, k -> createTargetDataListener(path, k));
        addTargetDataListener(path, targetListener, executor);
    }

    @Override
    public void removeDataListener(String path, DataListener listener ){
        ConcurrentMap<DataListener, TargetDataListener> dataListenerMap = listeners.get(path);
        if (dataListenerMap != null) {
            TargetDataListener targetListener = dataListenerMap.remove(listener);
            if(targetListener != null){
                removeTargetDataListener(path, targetListener);
            }
        }
    }

    @Override
    public void removeChildListener(String path, ChildListener listener) {
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
        if (listeners != null) {
            TargetChildListener targetListener = listeners.remove(listener);
            if (targetListener != null) {
                removeTargetChildListener(path, targetListener);
            }
        }
    }

    protected void stateChanged(int state) {
        for (StateListener sessionListener : getSessionListeners()) {
            sessionListener.stateChanged(state);
        }
    }

    /**
     * 该方法是关闭客户端，其中doClose方法也被抽象出。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            // 关闭
            doClose();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public void create(String path, String content, boolean ephemeral) {
        if (checkExists(path)) {
            delete(path);
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeral(path, content);
        } else {
            createPersistent(path, content);
        }
    }

    @Override
    public String getContent(String path) {
        if (!checkExists(path)) {
            return null;
        }
        return doGetContent(path);
    }

    //以下方法都是抽象方法

    /**
     * 关闭客户端
     */
    protected abstract void doClose();

    /**
     * 递归创建节点
     * @param path
     */
    protected abstract void createPersistent(String path);
    /**
     * 创建临时节点
     * @param path
     */
    protected abstract void createEphemeral(String path);

    protected abstract void createPersistent(String path, String data);

    protected abstract void createEphemeral(String path, String data);
    /**
     * 检测该节点是否存在
     * @param path
     * @return
     */
    protected abstract boolean checkExists(String path);
    /**
     * 创建子节点监听器
     * @param path
     * @param listener
     * @return
     */
    protected abstract TargetChildListener createTargetChildListener(String path, ChildListener listener);
    /**
     * 为子节点添加监听器
     * @param path
     * @param listener
     * @return
     */
    protected abstract List<String> addTargetChildListener(String path, TargetChildListener listener);


    protected abstract TargetDataListener createTargetDataListener(String path, DataListener listener);

    protected abstract void addTargetDataListener(String path, TargetDataListener listener);

    protected abstract void addTargetDataListener(String path, TargetDataListener listener, Executor executor);

    protected abstract void removeTargetDataListener(String path, TargetDataListener listener);
    /**
     * 移除子节点监听器
     * @param path
     * @param listener
     */
    protected abstract void removeTargetChildListener(String path, TargetChildListener listener);

    protected abstract String doGetContent(String path);

    /**
     * we invoke the zookeeper client to delete the node
     * @param path the node path
     */
    protected abstract void deletePath(String path);

}
