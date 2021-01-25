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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.URLStrParser;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_SEPARATOR_ENCODED;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;

/**
 * ZookeeperRegistry
 * 使用zk作为注册中心的优势：
 *   1、全量数据存储在内存中（效率高）
 *   2、支持集群（高可用）
 *   3、支持事件监听（数据发布订阅）
 *        体现：当提供程序意外停止时，注册表服务器可以自动删除其信息
 *             注册服务重新启动时，可以自动恢复所有注册数据和订阅请求
 *             会话过期后，可以自动恢复所有注册数据和订阅请求
 *
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

    // 默认zookeeper根节点
    private final static String DEFAULT_ROOT = "dubbo";

    // zookeeper根节点
    private final String root;

    // 服务接口集合
    private final Set<String> anyServices = new ConcurrentHashSet<>();

    // 监听器集合
    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<>();

    // zookeeper客户端实例
    private final ZookeeperClient zkClient;

    /**
     * 1、参数中ZookeeperTransporter是一个接口，CuratorZookeeperTransporter两个实现类，ZookeeperTransporter还是一个可扩展的接口，基于 Dubbo SPI Adaptive 机制，会根据url中携带的参数去选择用哪个实现类。
     * 2、上面我说明了dubbo在zookeeper节点层级有一层是root层，该层是通过group属性来设置的。
     * 3、给客户端添加一个监听器，当状态为重连的时候调用FailbackRegistry的恢复方法
     * @param url
     * @param zookeeperTransporter
     */
    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        // 获得url携带的分组配置，并且作为zookeeper的根节点
        String group = url.getParameter(GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;
        // 创建zookeeper client
        zkClient = zookeeperTransporter.connect(url);
        // 添加状态监听器，根据不同的状态调用不同的方法
        zkClient.addStateListener((state) -> {
            if (state == StateListener.RECONNECTED) {
                logger.warn("Trying to fetch the latest urls, in case there're provider changes during connection loss.\n" +
                        " Since ephemeral ZNode will not get deleted for a connection lose, " +
                        "there's no need to re-register url of this instance.");
                ZookeeperRegistry.this.fetchLatestAddresses();
            } else if (state == StateListener.NEW_SESSION_CREATED) {
                logger.warn("Trying to re-register urls and re-subscribe listeners of this instance to registry...");
                try {
                    ZookeeperRegistry.this.recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (state == StateListener.SESSION_LOST) {
                logger.warn("Url of this instance will be deleted from registry soon. " +
                        "Dubbo client will try to re-register once a new session is created.");
            } else if (state == StateListener.SUSPENDED) {

            } else if (state == StateListener.CONNECTED) {

            }
        });
    }

    /**
     *检测zookeeper是否连接，很简单，都是调用了zookeeper客户端封装好的方法。
     */

    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    /**
     * 检测zookeeper销毁连接，很简单，都是调用了zookeeper客户端封装好的方法。
     */
    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doRegister(URL url) {
        try {
            // 创建URL节点，也就是URL层的节点
            zkClient.create(toUrlPath(url), url.getParameter(DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnregister(URL url) {
        try {
            // 删除节点
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     *  Zookeeper
     *  ROOT                                    dubbo
     *                                        /
     *  Service                      com.pay.UserService <---------------------|
     *                              /                  \                       |
     *  Type                providers                   consumers              |
     *                    /     ^    \                   /                     |
     *  URL 10.21.0.3:20080     |   10.21.0.4:20080    10.23.0.9:9090          |
     *              ^           |                            ^                 |
     *              |           |                            |                 |
     *    privider(register)    (subscribe) consumer(register)          Moniter(subscribe)
     *
     *
     *
     * 本方法可以分两段来看，这里的实现把所有Service层发起的订阅以及指定的Service层发起的订阅分开处理。
     * 所有Service层类似于监控中心发起的订阅。
     * 指定的Service层发起的订阅可以看作是服务消费者的订阅。订阅的大致逻辑类似，不过还是有几个区别：
     *      1、所有Service层发起的订阅中的ChildListener是在在 Service 层发生变更时，才会做出解码，
     *         用anyServices属性判断是否是新增的服务，最后调用父类的subscribe订阅。
     *         而指定的Service层发起的订阅是在URL层发生变更的时候，调用notify，
     *         回调回调NotifyListener的逻辑，做到通知服务变更
     *      2、所有Service层发起的订阅中客户端创建的节点是Service节点，该节点为持久节点，
     *         而指定的Service层发起的订阅中创建的节点是Type节点，该节点也是持久节点。
     *         这里补充一下zookeeper的持久节点是节点创建后，就一直存在，直到有删除操作来主动清除这个节点，不会因为创建该节点的客户端会话失效而消失。
     *         而临时节点的生命周期和客户端会话绑定。也就是说，如果客户端会话失效，那么这个节点就会自动被清除掉。
     *         注意，这里提到的是会话失效，而非连接断开。另外，在临时节点下面不能创建子节点。
     *      3、指定的Service层发起的订阅中调用了两次notify，第一次是增量的通知，也就是只是通知这次增加的服务节点，
     *         而第二个是全量的通知。
     * @param url
     * @param listener
     */
    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            // 处理所有Service层发起的订阅，例如监控中心的订阅
            if (ANY_VALUE.equals(url.getServiceInterface())) {
                // 获得根目录
                String root = toRootPath();
                // 获得url对应的监听器集合
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                // 不存在就创建监听器集合
                ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> {
                    // 遍历现有的节点，如果现有的服务集合中没有该节点，则加入该节点，然后订阅该节点
                    for (String child : currentChilds) {
                        // 解码
                        child = URL.decode(child);
                        if (!anyServices.contains(child)) {
                            anyServices.add(child);
                            subscribe(url.setPath(child).addParameters(INTERFACE_KEY, child,
                                    Constants.CHECK_KEY, String.valueOf(false)), k);
                        }
                    }
                });
                // 创建service节点，该节点为持久节点
                zkClient.create(root, false);
                // 向zookeeper的service节点发起订阅，获得Service接口全名数组
                List<String> services = zkClient.addChildListener(root, zkListener);
                if (CollectionUtils.isNotEmpty(services)) {
                    // 遍历Service接口全名数组
                    for (String service : services) {
                        service = URL.decode(service);
                        anyServices.add(service);
                        // 发起该service层的订阅
                        subscribe(url.setPath(service).addParameters(INTERFACE_KEY, service,
                                Constants.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                // 处理指定 Service 层的发起订阅，例如服务消费者的订阅
                List<URL> urls = new ArrayList<>();
                // 遍历分类数组
                for (String path : toCategoriesPath(url)) {
                    // 获得监听器集合
                    ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                    // 如果没有则创建
                    // 通知服务变化 回调NotifyListener
                    // 重新获取节点监听器，保证一致性
                    ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> ZookeeperRegistry.this.notify(url, k, toUrlsWithEmpty(url, parentPath, currentChilds)));
                    // 创建type节点，该节点为持久节点
                    zkClient.create(path, false);
                    // 向zookeeper的type节点发起订阅
                    List<String> children = zkClient.addChildListener(path, zkListener);
                    if (children != null) {
                        // 加入到自子节点数据数组
                        urls.addAll(toUrlsWithEmpty(url, path, children));
                    }
                }
                // 通知数据变化
                notify(url, listener, urls);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 该方法分为两种情况，
     *      1、所有的Service发起的取消订阅
     *      2、还是指定的Service发起的取消订阅。
     * 可以看到所有的Service发起的取消订阅就直接移除了根目录下所有的监听器，
     * 而指定的Service发起的取消订阅是移除了该Service层下面的所有Type节点监听器。如果不太明白再回去看看前面的那个节点层级图。
     * @param url
     * @param listener
     */
    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        // 获得监听器集合
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            // 获得子节点的监听器
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
                // 如果为全部的服务接口，例如监控中心
                if (ANY_VALUE.equals(url.getServiceInterface())) {
                    // 获得根目录
                    String root = toRootPath();
                    // 移除监听器
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    // 遍历分类数组进行移除监听器
                    for (String path : toCategoriesPath(url)) {
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }
        }
    }

    /**
     * 该方法就是查询符合条件的已经注册的服务
     * @param url Query condition, is not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @return
     */
    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            // 遍历分组类别
            List<String> providers = new ArrayList<>();
            for (String path : toCategoriesPath(url)) {
                // 获得子节点
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            // 获得 providers 中，和 consumer 匹配的 URL 数组
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private String toRootDir() {
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    private String toRootPath() {
        return root;
    }

    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        // 如果是包括所有服务，则返回根节点
        if (ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
    }

    /**
     * 方法是获得分类数组，也就是url携带的服务下的所有Type节点数组。
     */
    private String[] toCategoriesPath(URL url) {
        String[] categories;
        // 如果url携带的分类配置为*，则创建包括所有分类的数组
        if (ANY_VALUE.equals(url.getParameter(CATEGORY_KEY))) {
            categories = new String[]{PROVIDERS_CATEGORY, CONSUMERS_CATEGORY, ROUTERS_CATEGORY, CONFIGURATORS_CATEGORY};
        } else {
            // 返回url携带的分类配置
            categories = url.getParameter(CATEGORY_KEY, new String[]{DEFAULT_CATEGORY});
        }
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            // 加上服务路径
            paths[i] = toServicePath(url) + PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    /**
     * 方法是获得分类路径，分类路径拼接规则：Root + Service + Type
     * @param url
     * @return
     */
    private String toCategoryPath(URL url) {
        return toServicePath(url) + PATH_SEPARATOR + url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
    }

    /**
     * 该方法是获得URL路径，拼接规则是Root + Service + Type + URL
     * @param url
     * @return
     */
    private String toUrlPath(URL url) {
        return toCategoryPath(url) + PATH_SEPARATOR + URL.encode(url.toFullString());
    }

    /**
     * toUrlsWithoutEmpty方法是获得 providers 中，和 consumer 匹配的 URL 数组，。通过这样的方式，可以处理类似服务提供者为空的情况
     * @param consumer
     * @param providers
     * @return
     */
    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<>();
        // 遍历服务提供者
        if (CollectionUtils.isNotEmpty(providers)) {
            for (String provider : providers) {
                // 解码
                // 把服务转化成url的形式
                if (provider.contains(PROTOCOL_SEPARATOR_ENCODED)) {
                    URL url = URLStrParser.parseEncodedStr(provider);
                    // 判断是否匹配，如果匹配， 则加入到集合中
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }

    /**
     * toUrlsWithEmpty方法是调用了第一个方法后增加了若不存在匹配，则创建 empty:// 的 URL返回  通过这样的方式，可以处理类似服务提供者为空的情况
     * @param consumer
     * @param path
     * @param providers
     * @return
     */
    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        // 返回和服务消费者匹配的服务提供者url
        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
        // 如果不存在，则创建`empty://` 的 URL返回
        if (urls == null || urls.isEmpty()) {
            int i = path.lastIndexOf(PATH_SEPARATOR);
            String category = i < 0 ? path : path.substring(i + 1);
            URL empty = URLBuilder.from(consumer)
                    .setProtocol(EMPTY_PROTOCOL)
                    .addParameter(CATEGORY_KEY, category)
                    .build();
            urls.add(empty);
        }
        return urls;
    }

    /**
     * When zookeeper connection recovered from a connection loss, it need to fetch the latest provider list.
     * re-register watcher is only a side effect and is not mandate.
     */
    private void fetchLatestAddresses() {
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Fetching the latest urls of " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

}
