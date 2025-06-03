#!/bin/bash

# 设置错误时退出
set -e

echo "开始部署 FT-UTXO 索引器..."

# 检查参数
if [ $# -ne 1 ]; then
    echo "使用方法: $0 <zip文件路径>"
    echo "示例: $0 utxo-indexer.zip"
    exit 1
fi

ZIP_FILE=$1

# 检查zip文件是否存在
if [ ! -f "$ZIP_FILE" ]; then
    echo "错误: 找不到文件 $ZIP_FILE"
    exit 1
fi

# 解压文件
echo "解压文件..."
unzip -q "$ZIP_FILE"

# 进入项目目录
echo "进入项目目录..."
cd utxo-indexer

# 停止旧容器
echo "停止旧容器..."
docker-compose -f deploy/docker-compose.ft.yml down || true

# 构建新镜像
echo "构建新镜像..."
docker-compose -f deploy/docker-compose.ft.yml build --no-cache

# 启动服务
echo "启动服务..."
docker-compose -f deploy/docker-compose.ft.yml up -d

# 检查服务状态
echo "检查服务状态..."
docker-compose -f deploy/docker-compose.ft.yml ps

echo "部署完成！"
echo "可以通过以下命令查看日志："
echo "docker-compose -f deploy/docker-compose.ft.yml logs -f"

# 清理zip文件
echo "清理zip文件..."
cd ..
rm -f "$ZIP_FILE" 