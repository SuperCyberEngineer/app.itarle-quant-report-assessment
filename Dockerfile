FROM maven:3.8.1-openjdk-11-slim AS Builder

WORKDIR /app

# ADD ./ /app

# RUN mvn -T 16 package

# RUN mvn -T 16 compile

RUN echo "running..."