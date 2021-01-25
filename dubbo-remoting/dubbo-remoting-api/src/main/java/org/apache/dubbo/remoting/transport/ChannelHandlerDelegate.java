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
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.remoting.ChannelHandler;

/**
 * 该类继承了ChannelHandler，从它的名字可以看出是ChannelHandler的代表，它就是作为装饰模式中的Component角色，
 * 后面讲到的AbstractChannelHandlerDelegate作为装饰模式中的Decorator角色。
 */
public interface ChannelHandlerDelegate extends ChannelHandler {
    //获得通道
    ChannelHandler getHandler();
}
