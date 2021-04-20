FROM rust:latest AS builder

WORKDIR /bin

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc
RUN chmod 755 /bin/mc

WORKDIR /app_build

RUN rustup component add rustfmt

COPY . .