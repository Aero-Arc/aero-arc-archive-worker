# syntax=docker/dockerfile:1.7
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/aero-arc-archive-worker ./cmd/aero-arc-archive-worker
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/aero-arc-archive-worker /usr/local/bin/aero-arc-archive-worker
EXPOSE 8092
ENTRYPOINT ["/usr/local/bin/aero-arc-archive-worker"]
