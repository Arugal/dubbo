package org.apache.dubbo.rpc;


import org.apache.dubbo.common.extension.ExtensionLoader;


public class ProxyFactory$Adaptive implements org.apache.dubbo.rpc.ProxyFactory {


    private static final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);
    private java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

    public java.lang.Object getProxy(org.apache.dubbo.rpc.Invoker arg0) throws org.apache.dubbo.rpc.RpcException {
        if (arg0 == null)
            throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument == null");
        if (arg0.getUrl() == null)
            throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument getUrl() == null");org.apache.dubbo.common.URL url = arg0.getUrl();
        String extName = url.getParameter("proxy", "javassist");
        if(extName == null)
            throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.ProxyFactory) name from url(" + url.toString() + ") use keys([proxy])");
        org.apache.dubbo.rpc.ProxyFactory extension = null;
        try {
            extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension(extName);
        }catch(Exception e){
            if (count.incrementAndGet() == 1) {
                logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.ProxyFactory, will use default extension javassist instead.", e);
            }
            extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension("javassist");
        }
        return extension.getProxy(arg0);
    }
    public java.lang.Object getProxy(org.apache.dubbo.rpc.Invoker arg0, boolean arg1) throws org.apache.dubbo.rpc.RpcException {
        if (arg0 == null)
            throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument == null");
        if (arg0.getUrl() == null)
            throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument getUrl() == null");org.apache.dubbo.common.URL url = arg0.getUrl();
        String extName = url.getParameter("proxy", "javassist");
        if(extName == null)
            throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.ProxyFactory) name from url(" + url.toString() + ") use keys([proxy])");
        org.apache.dubbo.rpc.ProxyFactory extension = null;
        try {
            extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension(extName);
        }catch(Exception e){
            if (count.incrementAndGet() == 1) {
                logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.ProxyFactory, will use default extension javassist instead.", e);
            }
            extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension("javassist");
        }
        return extension.getProxy(arg0, arg1);
    }
    public org.apache.dubbo.rpc.Invoker getInvoker(java.lang.Object arg0, java.lang.Class arg1, org.apache.dubbo.common.URL arg2) throws org.apache.dubbo.rpc.RpcException {
        if (arg2 == null)
            throw new IllegalArgumentException("url == null");
        org.apache.dubbo.common.URL url = arg2;
        String extName = url.getParameter("proxy", "javassist");
        if(extName == null)
            throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.ProxyFactory) name from url(" + url.toString() + ") use keys([proxy])");
        org.apache.dubbo.rpc.ProxyFactory extension = null;
        try {
            extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension(extName);
        }catch(Exception e){
            if (count.incrementAndGet() == 1) {
                logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.ProxyFactory, will use default extension javassist instead.", e);
            }
            extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension("javassist");
        }
        return extension.getInvoker(arg0, arg1, arg2);
    }
}