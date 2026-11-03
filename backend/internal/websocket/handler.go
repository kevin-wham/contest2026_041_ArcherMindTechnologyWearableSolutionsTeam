package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	mqttlib "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"github.com/gorilla/websocket"

	"elderly-health-backend/internal/shared/logger"
	"elderly-health-backend/internal/shared/middleware"
	"elderly-health-backend/internal/shared/mqtt"
	"elderly-health-backend/internal/shared/redis"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 开发环境允许所有来源，生产环境应限制
	},
}

// Client 表示单个 WebSocket 连接
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	userID    string
	role      string
	elderlyID *int64 // 管理员关注的老人ID列表（可选）
}

// Hub 管理所有 WebSocket 连接
type Hub struct {
	clients    map[*Client]bool // 所有已注册客户端
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte        // 全局广播
	rooms      map[int64]map[*Client]bool // 按 elderly_id 分组的房间
	mu         sync.RWMutex
	jwtSecret  string
	stopChan   chan struct{}
}

// WSMessage WebSocket 消息格式
type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

// validateToken 从 URL query 参数解析并验证 JWT token
func validateToken(r *http.Request, jwtSecret string) (*middleware.Claims, error) {
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" {
		return nil, fmt.Errorf("missing token parameter")
	}

	token, err := jwt.ParseWithClaims(tokenString, &middleware.Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(jwtSecret), nil
	})

	if err != nil {
		return nil, fmt.Errorf("invalid token: %v", err)
	}

	claims, ok := token.Claims.(*middleware.Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}

// HandleWebSocket HTTP Handler，升级连接为 WebSocket
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 1. JWT 鉴权（从全局 Hub 获取 jwt secret）
	if globalHub == nil {
		http.Error(w, "Service not ready", http.StatusServiceUnavailable)
		return
	}

	claims, err := validateToken(r, globalHub.jwtSecret)
	if err != nil {
		log.Printf("[WS] Auth failed: %v", err)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// 2. 升级为 WebSocket 连接
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade failed: %v", err)
		return
	}

	// 3. 创建 Client 并注册到 Hub
	client := &Client{
		hub:    globalHub,
		conn:   conn,
		send:   make(chan []byte, 256),
		userID: claims.UserID,
		role:   claims.Role,
	}

	globalHub.register <- client

	log.Printf("[WS] Client connected: userID=%s role=%s", client.userID, client.role)

	// 4. 启动读写 goroutine
	go client.writePump()
	go client.readPump()
}

// readPump 读取客户端消息（心跳 ping/pong）
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512) // 限制读缓冲区
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[WS] Read error (userID=%s): %v", c.userID, err)
			}
			break
		}

		// 处理客户端发送的消息（目前只处理 ping）
		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err == nil {
			if msg.Type == "ping" {
				c.send <- []byte(`{"type":"pong","timestamp":` + fmt.Sprintf("%d", time.Now().Unix()) + `}`)
			}
		}
	}
}

// writePump 向客户端写入消息
func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second) // 心跳间隔 < 60s 读超时
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// 批量写入队列中的其他消息
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte("\n"))
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// 全局 Hub 实例
var globalHub *Hub

// StartHub 启动 WebSocket Hub 并返回实例
func StartHub(jwtSecret string) *Hub {
	hub := &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte, 256),
		rooms:      make(map[int64]map[*Client]bool),
		jwtSecret:  jwtSecret,
		stopChan:   make(chan struct{}),
	}

	globalHub = hub

	go hub.run()

	// 订阅 MQTT topic（桥接到 WebSocket）
	go hub.subscribeMQTT()

	// 订阅 Redis 广播频道（HTTP 上报路径的实时推送）
	go hub.subscribeRedisBroadcast()

	logger.Log.Info("websocket-service Hub started with MQTT/Redis subscription")

	return hub
}

// run Hub 主循环
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

			// 如果是 admin 角色，加入全局广播组
			if client.role == "admin" || client.role == "super_admin" {
				logger.Log.Info("Admin WS client registered",
					zap.String("user_id", client.userID))
			}

			// 发送欢迎消息
			welcomeMsg, _ := json.Marshal(WSMessage{
				Type: "connected",
				Data: map[string]interface{}{
					"user_id": client.userID,
					"role":    client.role,
					"time":    time.Now().Format(time.RFC3339),
				},
			})
			client.send <- welcomeMsg

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				// 从所有房间移除
				for roomID, clients := range h.rooms {
					if _, ok := clients[client]; ok {
						delete(clients, client)
						if len(clients) == 0 {
							delete(h.rooms, roomID)
						}
					}
				}
			}
			h.mu.Unlock()
			logger.Log.Info("WS client disconnected",
				zap.String("user_id", client.userID))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// 客户端缓冲区满，关闭连接
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()

		case <-h.stopChan:
			return
		}
	}
}

// subscribeRedisBroadcast 订阅 Redis 广播频道并桥接到 WebSocket
// 用于 HTTP 上报路径（手表/模拟器 POST /v1/vitals/sign、/v1/alarms/report 等）
// 这些数据不经过 MQTT，需要各服务写库成功后主动 publish
func (h *Hub) subscribeRedisBroadcast() {
	if redis.Client == nil {
		logger.Log.Warn("Redis client not available, skipping WS broadcast subscription")
		return
	}

	pubsub := redis.Client.Subscribe(context.Background(), redis.ChannelWSBroadcast)
	defer pubsub.Close()

	if _, err := pubsub.Receive(context.Background()); err != nil {
		logger.Log.Error("Failed to subscribe Redis WS broadcast channel", zap.Error(err))
		return
	}
	logger.Log.Info("Subscribed Redis WS broadcast channel",
		zap.String("channel", redis.ChannelWSBroadcast))

	ch := pubsub.Channel()
	for {
		select {
		case <-h.stopChan:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if len(msg.Payload) == 0 {
				continue
			}
			// 消息体已是完整 WSMessage JSON，直接广播
			select {
			case h.broadcast <- []byte(msg.Payload):
			case <-h.stopChan:
				return
			}
			logger.Log.Debug("Bridged Redis→WS broadcast", zap.Int("size", len(msg.Payload)))
		}
	}
}

// subscribeMQTT 订阅 MQTT topic 并桥接到 WebSocket
func (h *Hub) subscribeMQTT() {
	// 使用全局 MQTT 客户端实例
	mc := mqtt.MQTTClient
	if mc == nil || !mc.IsConnected() {
		logger.Log.Warn("MQTT client not available, skipping WS subscription")
		return
	}

	topics := []string{
		mqtt.TopicDeviceOnline,
		mqtt.TopicDeviceHeartbeat,
		mqtt.TopicDeviceVital,
		mqtt.TopicDeviceAlarm,
		mqtt.TopicDeviceSignin,
		mqtt.TopicDeviceBatch,
	}

	// 将通配符格式转为实际订阅格式（paho.mqtt.golang 支持通配符）
	const mqttQoS byte = 0 // QoS 0: at most once

	for _, topicFormat := range topics {
		subTopic := mqttToWildcard(topicFormat)

		err := mc.Subscribe(subTopic, mqttQoS, h.handleMQTTMsg)
		if err != nil {
			logger.Log.Error("Failed to subscribe MQTT topic for WS bridge",
				zap.String("topic", subTopic),
				zap.Error(err))
		} else {
			logger.Log.Info("Subscribed MQTT topic for WS bridge",
				zap.String("topic", subTopic))
		}
	}
}

// mqttToWildcard 将 device/%s/xxx 格式转换为 device/+/xxx 通配符格式
func mqttToWildcard(format string) string {
	result := format
	for {
		idx := indexOf(result, "%s")
		if idx < 0 {
			break
		}
		result = result[:idx] + "+" + result[idx+2:]
	}
	return result
}

func indexOf(s string, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// handleMQTTMsg 处理收到的 MQTT 消息，转发到 WebSocket
func (h *Hub) handleMQTTMsg(c mqttlib.Client, m mqttlib.Message) {
	topic := m.Topic()
	payload := m.Payload()

	logger.Log.Debug("MQTT→WS bridge received message",
		zap.String("topic", topic),
		zap.ByteString("payload", payload))

	var wsMsg WSMessage

	switch {
	case matchTopic(topic, "device/", "/online"):
		// 设备上线
		var onlineData map[string]interface{}
		if json.Unmarshal(payload, &onlineData) != nil {
			onlineData = map[string]interface{}{
				"device_sn": string(payload),
				"status":    "online",
			}
		}
		deviceSN, _ := onlineData["device_sn"].(string)
		if deviceSN == "" {
			deviceSN = string(payload)
		}

		wsMsg = WSMessage{
			Type: "device_online",
			Data: map[string]interface{}{
				"device_sn": deviceSN,
				"status":    "online",
				"timestamp": time.Now().Unix(),
			},
		}

	case matchTopic(topic, "device/", "/heartbeat"):
		// 设备心跳（也是在线信号）
		var hbData mqtt.HeartbeatData
		if err := json.Unmarshal(payload, &hbData); err == nil {
			wsMsg = WSMessage{
				Type: "device_online",
				Data: map[string]interface{}{
					"device_sn":      hbData.DeviceSN,
					"status":         "online",
					"battery":        hbData.Battery,
					"signal_strength": hbData.SignalStrength,
					"timestamp":      time.Now().Unix(),
				},
			}
		} else {
			wsMsg = WSMessage{
				Type: "device_online",
				Data: map[string]interface{}{
					"device_sn": string(payload),
					"status":    "online",
					"timestamp": time.Now().Unix(),
				},
			}
		}

	case matchTopic(topic, "device/", "/alarm"):
		// 新报警 — 使用 map 安全解析（兼容 level/severity 字段名，避免 struct 指针 nil panic）
		var alarmRaw map[string]interface{}
		if json.Unmarshal(payload, &alarmRaw) == nil {
			// 从 raw map 中提取字段（安全方式，不会 panic）
			deviceSN, _ := alarmRaw["device_sn"].(string)
			elderlyID, _ := alarmRaw["elderly_id"].(string)
			alarmType, _ := alarmRaw["alarm_type"].(string)

			// 兼容 severity / level 两种字段名
			var severity string
			if s, ok := alarmRaw["severity"].(string); ok && s != "" {
				severity = s
			} else if l, ok := alarmRaw["level"].(string); ok {
				severity = l
			}

			// 时间戳兼容 timestamp / triggered_at
			var timeVal interface{}
			if t, ok := alarmRaw["timestamp"]; ok {
				timeVal = t
			} else if t, ok := alarmRaw["triggered_at"]; ok {
				timeVal = t
			}
			// 如果都没有，用当前时间
			triggeredAt := time.Now().Format(time.RFC3339)
			if timeVal != nil {
				if tsStr, ok := timeVal.(string); ok && tsStr != "" {
					triggeredAt = tsStr
				}
			}

			wsMsg = WSMessage{
				Type: "new_alarm",
				Data: map[string]interface{}{
					"alarm_id":     fmt.Sprintf("alarm-%s-%d", deviceSN, time.Now().UnixMilli()),
					"device_sn":    deviceSN,
					"elderly_id":   elderlyID,
					"alarm_type":   alarmType,
					"level":        severity,       // 前端期望的字段名
					"severity":     severity,       // 同时提供 severity
					"description":  fmt.Sprintf("%s报警 - %s", alarmTypeLabel(alarmType), alarmType),
					"triggered_at": triggeredAt,
				},
			}
			logger.Log.Info("WS bridge parsed alarm message",
				zap.String("device_sn", deviceSN),
				zap.String("type", alarmType),
				zap.String("level", severity),
			)
		} else {
			// JSON 解析失败时发送原始数据
			wsMsg = WSMessage{
				Type: "new_alarm",
				Data: map[string]interface{}{
					"raw_payload": string(payload),
				},
			}
			logger.Log.Warn("WS bridge failed to parse alarm payload, sending raw")
		}

	case matchTopic(topic, "device/", "/vital"):
		// 体征数据更新
		var vitalData mqtt.VitalData
		if err := json.Unmarshal(payload, &vitalData); err == nil {
			// 安全解引用指针字段
			hr := 0
			spo2 := float64(0)
			temp := float64(0)
			steps := 0
			if vitalData.HeartRate != nil {
				hr = *vitalData.HeartRate
			}
			if vitalData.SpO2 != nil {
				spo2 = *vitalData.SpO2
			}
			if vitalData.Temperature != nil {
				temp = *vitalData.Temperature
			}
			if vitalData.Steps != nil {
				steps = *vitalData.Steps
			}

			wsMsg = WSMessage{
				Type: "vital_update",
				Data: map[string]interface{}{
					"elderly_id":    vitalData.ElderlyID,
					"device_sn":     vitalData.DeviceSN,
					"heart_rate":    hr,
					"blood_oxygen":  spo2,
					"temperature":   temp,
					"steps":         steps,
					"timestamp":     time.Now().Unix(),
				},
			}
		} else {
			wsMsg = WSMessage{
				Type: "vital_update",
				Data: map[string]interface{}{
					"raw_payload": string(payload),
				},
			}
		}

	case matchTopic(topic, "device/", "/signin"):
		// 签到通知
		var signinData mqtt.SigninData
		if err := json.Unmarshal(payload, &signinData); err == nil {
			wsMsg = WSMessage{
				Type: "signin_notify",
				Data: map[string]interface{}{
					"elderly_id":  signinData.ElderlyID,
					"device_sn":  signinData.DeviceSN,
					"signin_time": time.Unix(signinData.Timestamp, 0).Format(time.RFC3339),
					"method":      signinData.Method,
				},
			}
		} else {
			wsMsg = WSMessage{
				Type: "signin_notify",
				Data: map[string]interface{}{
					"raw_payload": string(payload),
				},
			}
		}

	case matchTopic(topic, "device/", "/batch"):
		// 断网补传批次
		wsMsg = WSMessage{
			Type: "dashboard_refresh",
			Data: map[string]interface{}{
				"reason": "batch_data_received",
			},
		}

	default:
		logger.Log.Debug("Ignored unhandled MQTT topic in WS bridge",
			zap.String("topic", topic))
		return
	}

	// 序列化并广播到前端
	data, _ := json.Marshal(wsMsg)
	h.broadcast <- data
	logger.Log.Info("Bridged MQTT→WS broadcast",
		zap.String("ws_type", wsMsg.Type),
		zap.Int("payload_size", len(data)),
		zap.Int("connected_clients", h.GetConnectedClientsCount()))
}

// matchTopic 检查 topic 是否匹配模式 (如 device/D20260001/online 匹配 suffix="/online")
func matchTopic(topic, prefix, suffix string) bool {
	if len(topic) <= len(prefix)+len(suffix) {
		return false
	}
	hasPrefix := topic[:len(prefix)] == prefix
	hasSuffix := topic[len(topic)-len(suffix):] == suffix
	return hasPrefix && hasSuffix
}

// alarmTypeLabel 报警类型转中文标签
func alarmTypeLabel(t string) string {
	labels := map[string]string{
		"SOS":            "SOS求助",
		"FALL":           "跌倒检测",
		"VITAL_ABNORMAL": "体征异常",
		"SIGNIN_TIMEOUT":  "签到超时",
		"DEVICE_OFFLINE":  "设备离线",
	}
	if l, ok := labels[t]; ok {
		return l
	}
	return t
}

// Stop 停止 Hub
func (h *Hub) Stop() {
	select {
	case h.stopChan <- struct{}{}:
	default:
		// 已经在停止中
	}
}

// BroadcastToRoom 向指定房间（按 elderly_id）广播消息
func (h *Hub) BroadcastToRoom(elderlyID int64, data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if clients, ok := h.rooms[elderlyID]; ok {
		for client := range clients {
			select {
			case client.send <- data:
			default:
				// 缓冲区满
			}
		}
	}
}

// GetConnectedClientsCount 返回当前连接数（用于监控）
func (h *Hub) GetConnectedClientsCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
