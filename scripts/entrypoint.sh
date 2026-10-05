#!/bin/sh
# 容器入口：在单事务内完成建表/冲突检查/旧库补列回填后启动服务。
# 支持两个容器同时启动（后到者等待先到者升级完成）与中断后重启恢复；
# 升级失败（含资料冲突、等锁超时）init_db 非零退出，set -e 阻止对外服务。
set -e

echo "[entrypoint] 初始化数据库（单事务启动升级，支持并发启动与中断恢复）..."
python -c "from app.database import init_db; init_db(); print('[entrypoint] 数据库就绪')"

echo "[entrypoint] 启动: $*"
exec "$@"
