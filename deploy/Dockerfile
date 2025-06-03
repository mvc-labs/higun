# 使用多阶段构建
FROM golang:1.22-alpine AS builder

# 设置工作目录
WORKDIR /app

# 安装必要的构建工具
RUN apk add --no-cache git gcc musl-dev

# 复制go.mod和go.sum
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=1 GOOS=linux go build -o utxo-indexer main.go

# 使用轻量级的alpine作为运行环境
FROM alpine:latest

# 安装运行时依赖
RUN apk add --no-cache ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

# 创建数据目录
RUN mkdir -p /app/data

# 从builder阶段复制编译好的应用
COPY --from=builder /app/utxo-indexer /app/
COPY --from=builder /app/config.yaml /app/

# 设置工作目录
WORKDIR /app

# 暴露API端口
EXPOSE 8080

# 运行应用
CMD ["./utxo-indexer"] 