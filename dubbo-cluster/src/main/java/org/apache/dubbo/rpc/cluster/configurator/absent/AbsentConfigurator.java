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
package org.apache.dubbo.rpc.cluster.configurator.absent;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.cluster.configurator.AbstractConfigurator;

/**
 * AbsentConfigurator
 * 该配置方式就是当配置不存在的时候添加。
 */
public class AbsentConfigurator extends AbstractConfigurator {

    public AbsentConfigurator(URL url) {
        super(url);
    }

    @Override
    public URL doConfigure(URL currentUrl, URL configUrl) {
        // 当不存在时添加
        return currentUrl.addParametersIfAbsent(configUrl.getParameters());
    }

}
