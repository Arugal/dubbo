#!/bin/sh


export protocol_port=9999

java  -Ddubbo.protocol.dubbo.port=9998 -jar ./target/dubbo-demo-provider-2.7.0-SNAPSHOT.jar