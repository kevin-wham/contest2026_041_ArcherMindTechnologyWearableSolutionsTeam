package redis

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"elderly-health-backend/internal/shared/logger"
)

// ChannelWSBroadcast WebSocket 实时广播频道
// 任意微服务（体征/报警/签到 HTTP 上报）写库成功后向此频道 publish，
// websocket-service 订阅后转发给所有在线前端页面，实现"手表上报 → 页面秒级刷新"。
const ChannelWSBroadcast = "ws:broadcast"

// WSPayload 与 websocket-service 的 WSMessage 结构保持一致
type WSPayload struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

// PublishWS 向 WebSocket 广播频道发布一条实时消息（失败仅告警，不影响主流程）
func PublishWS(msgType string, data interface{}) {
	if Client == nil {
		return
	}

	payload, err := json.Marshal(WSPayload{Type: msgType, Data: data})
	if err != nil {
		logger.Log.Warn("PublishWS marshal failed", zap.String("type", msgType), zap.Error(err))
		return
	}

	if err := Client.Publish(context.Background(), ChannelWSBroadcast, payload).Err(); err != nil {
		logger.Log.Warn("PublishWS failed", zap.String("type", msgType), zap.Error(err))
		return
	}

	logger.Log.Debug("PublishWS sent", zap.String("type", msgType), zap.Int("size", len(payload)))
}
