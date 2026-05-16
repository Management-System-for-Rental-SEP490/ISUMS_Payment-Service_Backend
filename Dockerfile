FROM amazoncorretto:25-al2023-headless

WORKDIR /app

ARG XMX=512m
ENV JAVA_OPTS="-Xms256m -Xmx${XMX} -XX:+UseContainerSupport"

COPY build/libs/*.jar app.jar

RUN mkdir -p /app/logs

EXPOSE 8089

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
