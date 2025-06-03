#!/bin/bash

# 设置错误时退出
set -e

echo "开始本地调试 FT-UTXO 索引器..."

# 使用本地配置文件运行
echo "使用本地配置文件启动服务..."
go run ../apps/ft-main/main.go --config config_mvc_ft_local.yaml

echo "服务已停止" 