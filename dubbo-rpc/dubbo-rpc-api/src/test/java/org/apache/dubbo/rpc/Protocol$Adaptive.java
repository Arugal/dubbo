package org.apache.dubbo.rpc;


import org.apache.dubbo.common.extension.ExtensionLoader;


public class Protocol$Adaptive implements org.apache.dubbo.rpc.Protocol {


    private static final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);
    private java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

    public void destroy() {
        throw new UnsupportedOperationException("method public abstract void org.apache.dubbo.rpc.Protocol.destroy() of " +
                "interface org.apache.dubbo.rpc.Protocol is not adaptive method!");
    }
    public int getDefaultPort() {
        throw new UnsupportedOperationException("method public abstract int org.apache.dubbo.rpc.Protocol.getDefaultPort() " +
                "of interface org.apache.dubbo.rpc.Protocol is not adaptive method!");
    }
    public org.apache.dubbo.rpc.Exporter export(org.apache.dubbo.rpc.Invoker arg0) throws org.apache.dubbo.rpc.RpcException {
        if (arg0 == null)
            throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument == null");
        if (arg0.getUrl() == null)
            throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument getUrl() == null");org.apache.dubbo.common.URL url = arg0.getUrl();
        // 默认为 dubbo 协议
        String extName = ( url.getProtocol() == null ? "dubbo" : url.getProtocol() );
        if(extName == null)
            throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.Protocol) name from url(" + url.toString() + ") use keys([protocol])");
        org.apache.dubbo.rpc.Protocol extension = null;
        try {
            // 通过 SPI 加载
            extension = (org.apache.dubbo.rpc.Protocol)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.Protocol.class).getExtension(extName);
        }catch(Exception e){
            if (count.incrementAndGet() == 1) {
                logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.Protocol, will use default extension dubbo instead.", e);
            }
            extension = (org.apache.dubbo.rpc.Protocol)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.Protocol.class).getExtension("dubbo");
        }
        return extension.export(arg0);
    }
    public org.apache.dubbo.rpc.Invoker refer(java.lang.Class arg0, org.apache.dubbo.common.URL arg1) throws org.apache.dubbo.rpc.RpcException {
        if (arg1 == null)
            throw new IllegalArgumentException("url == null");
        org.apache.dubbo.common.URL url = arg1;
        String extName = ( url.getProtocol() == null ? "dubbo" : url.getProtocol() );
        if(extName == null)
            throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.Protocol) name from url(" + url.toString() + ") use keys([protocol])");
        org.apache.dubbo.rpc.Protocol extension = null;
        try {
            extension = (org.apache.dubbo.rpc.Protocol)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.Protocol.class).getExtension(extName);
        }catch(Exception e){
            if (count.incrementAndGet() == 1) {
                logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.Protocol, will use default extension dubbo instead.", e);
            }
            extension = (org.apache.dubbo.rpc.Protocol)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.Protocol.class).getExtension("dubbo");
        }
        return extension.refer(arg0, arg1);
    }
}