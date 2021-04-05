FROM rust:alpine AS builder

WORKDIR /app_build
RUN apk -U upgrade && apk add openssl-dev && apk add --no-cache musl-dev && apk add protoc

RUN rustup target add x86_64-unknown-linux-musl
RUN rustup component add rustfmt

COPY . .
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM scratch

WORKDIR /app
COPY --from=builder /app_build/target/x86_64-unknown-linux-musl/release/core_server .
WORKDIR /

COPY config .

CMD ["/app/core_server"]