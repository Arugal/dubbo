package org.apache.dubbo.demo.listener.expoeter;

import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.ExporterListener;
import org.apache.dubbo.rpc.RpcException;

/**
 * @author: zhangwei
 * @date: 下午9:48/2018/8/27
 */
public class DemoExpoeterListener implements ExporterListener {
    @Override
    public void exported(Exporter<?> exporter) throws RpcException {
        System.out.println("exported:"+exporter.getInvoker().getInterface().getName());
    }

    @Override
    public void unexported(Exporter<?> exporter) {
        System.out.println("unexported:"+exporter.getInvoker().getInterface().getName());

    }
}
