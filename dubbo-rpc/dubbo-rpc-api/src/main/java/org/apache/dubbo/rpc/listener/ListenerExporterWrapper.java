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
package org.apache.dubbo.rpc.listener;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.ExporterListener;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;

/**
 * ListenerExporter
 * 该类是服务暴露监听器包装类。
 */
public class ListenerExporterWrapper<T> implements Exporter<T> {

    //用到了装饰模式，其中很多实现方法直接调用了exporter的方法。
    private static final Logger logger = LoggerFactory.getLogger(ListenerExporterWrapper.class);
    /**
     * 服务暴露者
     */
    private final Exporter<T> exporter;

    /**
     * 服务暴露监听者集合
     */
    private final List<ExporterListener> listeners;

    /**
     * 该方法中对于每个服务暴露进行监听。
     * @param exporter
     * @param listeners
     */
    public ListenerExporterWrapper(Exporter<T> exporter, List<ExporterListener> listeners) {
        if (exporter == null) {
            throw new IllegalArgumentException("exporter == null");
        }
        this.exporter = exporter;
        this.listeners = listeners;
        if (CollectionUtils.isNotEmpty(listeners)) {
            RuntimeException exception = null;
            // 遍历服务暴露监听集合
            for (ExporterListener listener : listeners) {
                if (listener != null) {
                    try {
                        // 暴露服务监听
                        listener.exported(this);
                    } catch (RuntimeException t) {
                        logger.error(t.getMessage(), t);
                        exception = t;
                    }
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
    }

    @Override
    public Invoker<T> getInvoker() {
        return exporter.getInvoker();
    }

    /**
     * 该方法是对每个取消服务暴露的监听。
     */
    @Override
    public void unexport() {
        try {
            // 取消暴露
            exporter.unexport();
        } finally {
            if (CollectionUtils.isNotEmpty(listeners)) {
                RuntimeException exception = null;
                // 遍历监听集合
                for (ExporterListener listener : listeners) {
                    if (listener != null) {
                        try {
                            // 监听取消暴露
                            listener.unexported(this);
                        } catch (RuntimeException t) {
                            logger.error(t.getMessage(), t);
                            exception = t;
                        }
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }
        }
    }

}
