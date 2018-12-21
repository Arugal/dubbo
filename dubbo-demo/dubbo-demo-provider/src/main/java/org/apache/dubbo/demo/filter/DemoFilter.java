package org.apache.dubbo.demo.filter;

import org.apache.dubbo.rpc.*;

/**
 * @author: zhangwei
 * @date: 下午11:13/2018/8/26
 */
public class DemoFilter implements Filter {


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        System.out.println("Pass By DemoFilter");
        return invoker.invoke(invocation);
    }
}
