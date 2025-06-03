#!/bin/bash

# 设置错误时退出
set -e

echo "开始部署 FT-UTXO 索引器..."

# 停止并删除旧容器（如果存在）
docker-compose -f docker-compose.ft.yml down || true

# 构建新镜像
echo "构建新镜像..."
docker-compose -f docker-compose.ft.yml build

# 启动服务
echo "启动服务..."
docker-compose -f docker-compose.ft.yml up -d

# 检查服务状态
echo "检查服务状态..."
docker-compose -f docker-compose.ft.yml ps

echo "部署完成！"
echo "可以通过以下命令查看日志："
echo "docker-compose -f docker-compose.ft.yml logs -f" 