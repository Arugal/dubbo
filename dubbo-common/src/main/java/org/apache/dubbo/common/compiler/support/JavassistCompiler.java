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
package org.apache.dubbo.common.compiler.support;

import org.apache.dubbo.common.utils.ClassHelper;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JavassistCompiler. (SPI, Singleton, ThreadSafe)
 */
public class JavassistCompiler extends AbstractCompiler {

    //  import
    private static final Pattern IMPORT_PATTERN = Pattern.compile("import\\s+([\\w\\.\\*]+);\n");

    // extends
    private static final Pattern EXTENDS_PATTERN = Pattern.compile("\\s+extends\\s+([\\w\\.]+)[^\\{]*\\{\n");

    // implements
    private static final Pattern IMPLEMENTS_PATTERN = Pattern.compile("\\s+implements\\s+([\\w\\.]+)\\s*\\{\n");

    // method
    private static final Pattern METHODS_PATTERN = Pattern.compile("\n(private|public|protected)\\s+");

    //
    private static final Pattern FIELD_PATTERN = Pattern.compile("[^\n]+=[^\n]+;");

    @Override
    public Class<?> doCompile(String name, String source) throws Throwable {
        // 获得类名
        int i = name.lastIndexOf('.');
        String className = i < 0 ? name : name.substring(i + 1);

        // 创建 ClassPool 对象
        ClassPool pool = new ClassPool(true);
        // 设置类搜索路径
        pool.appendClassPath(new LoaderClassPath(ClassHelper.getCallerClassLoader(getClass())));

        // match import
        Matcher matcher = IMPORT_PATTERN.matcher(source);
        List<String> importPackages = new ArrayList<String>();
        Map<String, String> fullNames = new HashMap<String, String>();
        while (matcher.find()) {
            String pkg = matcher.group(1);
            if (pkg.endsWith(".*")) { // 引入整个包下的类/接口
                String pkgName = pkg.substring(0, pkg.length() - 2);
                pool.importPackage(pkgName);
                importPackages.add(pkgName);
            } else {// 引入指定的类/接口
                int pi = pkg.lastIndexOf('.');
                if (pi > 0) {
                    String pkgName = pkg.substring(0, pi);
                    pool.importPackage(pkgName);
                    importPackages.add(pkgName);
                    fullNames.put(pkg.substring(pi + 1), pkg);
                }
            }
        }
        String[] packages = importPackages.toArray(new String[0]);

        // match expent
        matcher = EXTENDS_PATTERN.matcher(source);
        CtClass cls;
        if (matcher.find()) {
            String extend = matcher.group(1).trim();
            String extendClass;
            if (extend.contains(".")) { // 内嵌的类 例如: extends A.B
                extendClass = extend;
            } else if (fullNames.containsKey(extend)) {  // 引入指定的类
                extendClass = fullNames.get(extend);
            } else {// 引入整个包下的类
                extendClass = ClassUtils.forName(packages, extend).getName();
            }
            // 创建 CtClass 对象
            cls = pool.makeClass(name, pool.get(extendClass));
        } else {
            // 创建 CtClass 对象
            cls = pool.makeClass(name);
        }

        // match implements
        matcher = IMPLEMENTS_PATTERN.matcher(source);
        if (matcher.find()) {
            String[] ifaces = matcher.group(1).trim().split("\\,");
            for (String iface : ifaces) {
                iface = iface.trim();
                String ifaceClass;
                if (iface.contains(".")) { // 内嵌的接口 implements A.B
                    ifaceClass = iface;
                } else if (fullNames.containsKey(iface)) { // 引用指定的接口
                    ifaceClass = fullNames.get(iface);
                } else { // 引用整个包下的
                    ifaceClass = ClassUtils.forName(packages, iface).getName();
                }
                // 添加接口
                cls.addInterface(pool.get(ifaceClass));
            }
        }
        // 获得类中的内容 既首末 {} 的内容
        String body = source.substring(source.indexOf("{") + 1, source.length() - 1);
        // 匹配 method, 使用分隔的方式，实际上，分隔出来的不仅仅有方法
        String[] methods = METHODS_PATTERN.split(body);
        for (String method : methods) {
            method = method.trim();
            if (method.length() > 0) {
                if (method.startsWith(className)) { // 构造方法
                    cls.addConstructor(CtNewConstructor.make("public " + method, cls));
                } else if (FIELD_PATTERN.matcher(method).matches()) { // 变量
                    cls.addField(CtField.make("private " + method, cls));
                } else { // 防范
                    cls.addMethod(CtNewMethod.make("public " + method, cls));
                }
            }
        }
        return cls.toClass(ClassHelper.getCallerClassLoader(getClass()), JavassistCompiler.class.getProtectionDomain());
    }

}
