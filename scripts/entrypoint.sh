#!/bin/sh
# 容器入口：初始化数据库表结构（幂等，支持多实例并发启动）后启动服务。
set -e

echo "[entrypoint] 初始化数据库（幂等；并发启动仅一个实例执行升级）..."
python -c "from app.database import init_db; init_db(); print('[entrypoint] 数据库就绪')"

echo "[entrypoint] 启动: $*"
exec "$@"
