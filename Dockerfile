FROM rust:latest AS builder

WORKDIR /app_build

RUN rustup component add rustfmt

COPY . .

RUN cargo build --release

FROM debian:buster-slim

RUN apt-get -y update && apt-get -y upgrade && apt-get install -y libssl-dev

RUN useradd -ms /bin/bash appuser

USER appuser
WORKDIR /app
COPY --from=builder /app_build/target/release/core_server .
WORKDIR /

COPY config .

CMD ["/app/core_server"]