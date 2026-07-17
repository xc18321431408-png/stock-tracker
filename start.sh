#!/bin/bash
cd "$(dirname "$0")"

echo "============================================"
echo "  美股板块追踪 + 量化交易平台"
echo "  启动中..."
echo "============================================"

# Kill old processes
lsof -ti:8080 | xargs kill -9 2>/dev/null
pkill -f "lt --port 8080" 2>/dev/null

# Start Flask server in background
python3 app.py &
FLASK_PID=$!
sleep 3

# Check if Flask started
if ! curl -s http://localhost:8080/api/health > /dev/null 2>&1; then
    echo "❌ Flask 启动失败！"
    exit 1
fi
echo "✅ 本地服务已启动: http://localhost:8080"

# Start localtunnel
echo "🌐 正在创建外网隧道..."
lt --port 8080 --print-requests 2>&1 | while read line; do
    echo "$line"
    if echo "$line" | grep -q "your url is:"; then
        URL=$(echo "$line" | grep -o 'https://[^ ]*\.loca\.lt')
        echo ""
        echo "============================================"
        echo "  🌍 外网访问地址:"
        echo "  $URL"
        echo "============================================"
        echo ""
        echo "  按 Ctrl+C 停止所有服务"
        echo ""
    fi
done

# Cleanup on exit
kill $FLASK_PID 2>/dev/null
echo "服务已停止"
