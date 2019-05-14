FROM openjdk:8-jre-alpine

COPY target/dubbo-demo-provider-2.7.0-SNAPSHOT.jar /home/provider.jar

COPY target/lib /home/lib

WORKDIR /home

ENTRYPOINT ["java", "-jar"]

CMD ["./provider.jar"]