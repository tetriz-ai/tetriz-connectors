# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

# Builds the tetriz-connectors binary into a small Alpine image.
FROM docker.io/library/golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/tetriz-connectors ./cmd/tetriz-connectors

FROM docker.io/library/alpine:3.24@sha256:c6b073f8e9e58d881f6ffba6ebf3b9ebd60b9e7d6e8337018c0e68bb5c356873
LABEL org.opencontainers.image.source="https://github.com/tetriz-ai/tetriz-connectors" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.description="Connector runner and vendor simulator"
# A fixed numeric id, so a host volume can be chowned to it and USER
# resolves without the image's passwd file.
RUN addgroup -S -g 10001 tetriz && adduser -S -u 10001 -G tetriz tetriz && mkdir -p /data && chown 10001:10001 /data
COPY --from=build /out/tetriz-connectors /usr/local/bin/tetriz-connectors
COPY LICENSE NOTICE /usr/share/doc/tetriz-connectors/
COPY third_party /usr/share/doc/tetriz-connectors/third_party
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["tetriz-connectors"]
CMD ["serve"]
