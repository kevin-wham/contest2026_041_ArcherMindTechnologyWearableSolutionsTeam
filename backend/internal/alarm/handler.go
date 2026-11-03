package alarm

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"elderly-health-backend/internal/shared/database"
	apperrors "elderly-health-backend/internal/shared/errors"
	"elderly-health-backend/internal/shared/logger"
	"elderly-health-backend/internal/shared/middleware"
	"elderly-health-backend/internal/shared/redis"
	"elderly-health-backend/internal/shared/utils"
)

// RegisterRoutes 注册报警服务路由
func RegisterRoutes(r *gin.RouterGroup, jwtSecret string) {
	auth := r.Group("")
	auth.Use(middleware.JWTAuth(jwtSecret))
	{
		auth.GET("/alarms", ListAlarms)
		auth.GET("/alarms/:id", GetAlarm)
		auth.POST("/alarms/:id/confirm", ConfirmAlarm)
		auth.POST("/alarms/:id/resolve", ResolveAlarm)
		auth.POST("/alarms/:id/escalate", EscalateAlarm)
		auth.POST("/alarms/:id/emergency", EmergencyCall)
		auth.GET("/alarms/stats", AlarmStats)

		// 报警规则配置（管理员）
		auth.GET("/alarm-rules", ListAlarmRules)
		auth.PUT("/alarm-rules/:id", UpdateAlarmRule)
	}

	// 设备端报警上报（无需 JWT，模拟器调试用）
	r.POST("/alarms/report", HandleAlarmReport)
}

// ============ 报警队列消费者 ============

func StartAlarmQueueConsumer() {
	logger.Log.Info("Starting alarm queue consumer")

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			processAlarmQueue()
		}
	}()
}

func processAlarmQueue() {
	ctx := context.Background()

	// 从 Sorted Set 弹出最高优先级报警
	alarms, err := redis.Client.ZRevRange(ctx, redis.KeyAlarmQueue, 0, 0).Result()
	if err != nil || len(alarms) == 0 {
		return
	}

	alarmID := alarms[0]

	// 获取分布式锁，防止重复处理
	lockKey := "alarm:process:" + alarmID
	locked, err := redis.CacheHelper.Lock(ctx, lockKey, 30*time.Second)
	if err != nil || !locked {
		return
	}
	defer redis.CacheHelper.Unlock(ctx, lockKey)

	// 从队列移除
	redis.Client.ZRem(ctx, redis.KeyAlarmQueue, alarmID)

	// 处理报警：分析等级、触发通知
	var alarmLevel, alarmType, elderlyID string
	err = database.DB.QueryRow(
		"SELECT alarm_level, alarm_type, elderly_id FROM alarm_records WHERE id = ? AND status = 'pending'",
		alarmID,
	).Scan(&alarmLevel, &alarmType, &elderlyID)

	if err != nil {
		return
	}

	logger.Log.Info("Processing alarm",
		zap.String("id", alarmID),
		zap.String("level", alarmLevel),
		zap.String("type", alarmType),
	)

	// 根据等级触发相应处理
	switch alarmLevel {
	case "P0":
		triggerEmergency(alarmID, elderlyID)
	case "P1":
		triggerImmediateNotify(alarmID, elderlyID)
	case "P2", "P3":
		triggerRoutineNotify(alarmID, elderlyID)
	}
}

// ============ 报警升级检查（定时任务） ============

func CheckEscalation() {
	// P0 超过 5 秒未处理 → 自动联动 120
	rows, err := database.DB.Query(
		`SELECT id, elderly_id FROM alarm_records 
		 WHERE alarm_level='P0' AND status='pending' 
		 AND created_at < DATE_SUB(NOW(), INTERVAL 5 SECOND)`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id, elderlyID string
		rows.Scan(&id, &elderlyID)
		database.DB.Exec(
			"UPDATE alarm_records SET escalated_to='emergency_120', status='escalated', updated_at=NOW() WHERE id=?",
			id,
		)
		logger.Log.Warn("P0 alarm auto-escalated to 120", zap.String("id", id))
	}

	// P1 超过 1 分钟未处理 → 升级为 P0
	database.DB.Exec(
		`UPDATE alarm_records SET alarm_level='P0', updated_at=NOW()
		 WHERE alarm_level='P1' AND status='pending'
		 AND created_at < DATE_SUB(NOW(), INTERVAL 1 MINUTE)`)
}

func CleanupResolvedAlarms() {
	// 清理超过 30 天的已解决报警（移到归档表或标记）
	database.DB.Exec(
		`UPDATE alarm_records SET status='archived' 
		 WHERE (status='resolved' OR status='escalated')
		 AND updated_at < DATE_SUB(NOW(), INTERVAL 30 DAY)`)
}

// ============ 通知触发 ============

func triggerEmergency(alarmID, elderlyID string) {
	logger.Log.Error("P0 EMERGENCY - Triggering 120",
		zap.String("alarm_id", alarmID),
		zap.String("elderly_id", elderlyID),
	)

	// 获取老人信息
	var name, address, emergencyPhone string
	database.DB.QueryRow(
		`SELECT name, address, emergency_phone FROM elderly_profiles WHERE id=?`,
		elderlyID,
	).Scan(&name, &address, &emergencyPhone)

	// TODO: 调用 120 急救接口
	// TODO: 发送紧急通知给所有绑定子女
	// TODO: 通知社区工作人员

	logger.Log.Info("Emergency notification dispatched",
		zap.String("name", name),
		zap.String("phone", emergencyPhone),
	)
}

func triggerImmediateNotify(alarmID, elderlyID string) {
	logger.Log.Warn("P1 ALARM - Immediate notification",
		zap.String("alarm_id", alarmID),
	)

	// TODO: 发送 Push 通知给子女
	// TODO: 发送短信通知
}

func triggerRoutineNotify(alarmID, elderlyID string) {
	logger.Log.Info("P2/P3 ALARM - Routine notification",
		zap.String("alarm_id", alarmID),
	)

	// TODO: 生成通知记录，后续汇总推送
}

// ============ REST 接口 ============

func ListAlarms(c *gin.Context) {
	page := utils.Pagination{}
	c.ShouldBindQuery(&page)
	page.Normalize()

	level := c.Query("level")
	status := c.Query("status")
	elderlyID := c.Query("elderly_id")

	// 构建 WHERE 条件
	where := " WHERE 1=1"
	args := []interface{}{}

	if level != "" {
		where += " AND a.alarm_level = ?"
		args = append(args, level)
	}
	if status != "" {
		where += " AND a.status = ?"
		args = append(args, status)
	}
	if elderlyID != "" {
		where += " AND a.elderly_id = ?"
		args = append(args, elderlyID)
	}

	// 查询总数
	var total int64
	countQuery := `SELECT COUNT(*) FROM alarm_records a` + where
	database.DB.QueryRow(countQuery, args...).Scan(&total)

	// 查询数据
	query := `SELECT a.id, a.elderly_id, e.name as elderly_name, a.alarm_type,
		a.alarm_level, a.alarm_source, a.status, a.description, a.created_at, a.updated_at
		FROM alarm_records a LEFT JOIN elderly_profiles e ON a.elderly_id = e.id` +
		where +
		` ORDER BY FIELD(a.alarm_level,'P0','P1','P2','P3'), a.created_at DESC, a.id ASC LIMIT ? OFFSET ?`
	args = append(args, page.PageSize, page.Offset())

	rows, err := database.DB.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}
	defer rows.Close()

	var list []gin.H
	for rows.Next() {
		var id, eID, aType, aLevel, aSource, status, desc string
		var eName sql.NullString
		var createdAt, updatedAt time.Time
		rows.Scan(&id, &eID, &eName, &aType, &aLevel, &aSource, &status, &desc, &createdAt, &updatedAt)
		list = append(list, gin.H{
			"id": id, "elderly_id": eID, "elderly_name": eName.String,
			"alarm_type": aType, "level": aLevel, "alarm_source": aSource,
			"status": status, "description": desc,
			"triggered_at": createdAt, "created_at": createdAt, "updated_at": updatedAt,
		})
	}

	c.JSON(http.StatusOK, utils.SuccessPage(list, total, page.Page, page.PageSize))
}

func GetAlarm(c *gin.Context) {
	id := c.Param("id")

	var a struct {
		ID, ElderlyID, AlarmType, AlarmLevel, AlarmSource string
		Status, Description sql.NullString
		VitalSnapshotJSON, LocationJSON sql.NullString
		HandledBy, ResolvedBy, EscalatedTo sql.NullString
		CreatedAt, UpdatedAt time.Time
	}

	err := database.DB.QueryRow(
		`SELECT id, elderly_id, alarm_type, alarm_level, alarm_source,
		 status, description, vital_snapshot_json, location_json,
		 handled_by, resolved_by, escalated_to, created_at, updated_at
		 FROM alarm_records WHERE id = ?`, id,
	).Scan(&a.ID, &a.ElderlyID, &a.AlarmType, &a.AlarmLevel, &a.AlarmSource,
		&a.Status, &a.Description, &a.VitalSnapshotJSON, &a.LocationJSON,
		&a.HandledBy, &a.ResolvedBy, &a.EscalatedTo, &a.CreatedAt, &a.UpdatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, utils.Error(apperrors.ErrCodeAlarmNotFound, "报警不存在"))
		return
	}

	// 获取老人名字（用于详情展示）
	var elderlyName sql.NullString
	database.DB.QueryRow(
		`SELECT name FROM elderly_profiles WHERE id = ?`, a.ElderlyID,
	).Scan(&elderlyName)

	// 解析 vital_snapshot_json
	var vitalSnapshot gin.H
	if a.VitalSnapshotJSON.Valid && a.VitalSnapshotJSON.String != "" {
		_ = json.Unmarshal([]byte(a.VitalSnapshotJSON.String), &vitalSnapshot)
	}

	// 获取处理日志
	logRows, _ := database.DB.Query(
		`SELECT l.action, l.operator_id, l.operator_role, l.note, l.created_at,
		 IFNULL(a.real_name, l.operator_id) as handler_name
		 FROM alarm_handling_logs l
		 LEFT JOIN admin_users a ON l.operator_id = a.id
		 WHERE l.alarm_id = ? ORDER BY l.created_at`, id,
	)
	defer logRows.Close()

	var logs []gin.H
	for logRows.Next() {
		var action, opID, opRole, note, handlerName string
		var createdAt time.Time
		logRows.Scan(&action, &opID, &opRole, &note, &createdAt, &handlerName)
		logs = append(logs, gin.H{
			"action": action, "handler_id": opID, "handler_name": handlerName,
			"operator_role": opRole, "comment": note, "created_at": createdAt,
		})
	}

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"id":              a.ID,
		"elderly_id":      a.ElderlyID,
		"elderly_name":    elderlyName.String,
		"alarm_type":      a.AlarmType,
		"level":           a.AlarmLevel,
		"alarm_source":    a.AlarmSource,
		"status":          a.Status.String,
		"description":     a.Description.String,
		"vital_snapshot":  vitalSnapshot,
		"triggered_at":    a.CreatedAt,
		"created_at":      a.CreatedAt,
		"updated_at":      a.UpdatedAt,
		"resolved_at":     a.UpdatedAt, // 简化：用 updated_at 作为 resolved_at
		"handler_id":      a.HandledBy.String,
		"handler_name":    a.HandledBy.String,
		"handling_logs":   logs,
		}))
}

type HandleAlarmRequest struct {
	Note string `json:"note"`
}

func ConfirmAlarm(c *gin.Context) {
	handleAlarm(c, "confirmed")
}

func ResolveAlarm(c *gin.Context) {
	handleAlarm(c, "resolved")
}

func EscalateAlarm(c *gin.Context) {
	id := c.Param("id")
	operatorID, _ := c.Get("user_id")

	database.DB.Exec(
		"UPDATE alarm_records SET status='escalated', escalated_to='community', updated_at=NOW() WHERE id=?",
		id,
	)
	recordLog(id, "escalated", operatorID.(string), "community", "报警已升级")

	c.JSON(http.StatusOK, utils.Success(gin.H{"message": "报警已升级"}))
}

func EmergencyCall(c *gin.Context) {
	id := c.Param("id")
	operatorID, _ := c.Get("user_id")

	var elderlyID string
	database.DB.QueryRow("SELECT elderly_id FROM alarm_records WHERE id=?", id).Scan(&elderlyID)

	// 联动 120
	triggerEmergency(id, elderlyID)

	database.DB.Exec(
		"UPDATE alarm_records SET status='escalated', escalated_to='emergency_120', updated_at=NOW() WHERE id=?",
		id,
	)
	recordLog(id, "emergency_120", operatorID.(string), "admin", "已拨打120急救")

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"message": "已拨打120急救电话，救护车将尽快到达",
		"ambulance": gin.H{
			"status":     "dispatched",
			"eta_minutes": 15,
			"tracking":    "https://emergency.example.com/track/" + id,
		},
	}))
}

func handleAlarm(c *gin.Context, action string) {
	id := c.Param("id")
	operatorID, _ := c.Get("user_id")
	operatorRole, _ := c.Get("role")

	var req HandleAlarmRequest
	c.ShouldBindJSON(&req)

	status := action
	if action == "confirmed" {
		status = "confirmed"
	}

	database.DB.Exec(
		`UPDATE alarm_records SET status=?, 
		 handled_by=CASE WHEN ?='confirmed' THEN ? ELSE handled_by END,
		 resolved_by=CASE WHEN ?='resolved' THEN ? ELSE resolved_by END,
		 handled_at=CASE WHEN ?='confirmed' THEN NOW() ELSE handled_at END,
		 resolved_at=CASE WHEN ?='resolved' THEN NOW() ELSE resolved_at END,
		 updated_at=NOW() WHERE id=?`,
		status, action, operatorID, action, operatorID,
		action, action, id,
	)

	recordLog(id, action, operatorID.(string), operatorRole.(string), req.Note)

	c.JSON(http.StatusOK, utils.Success(gin.H{"message": "处理成功"}))
}

func recordLog(alarmID, action, operatorID, operatorRole, note string) {
	database.DB.Exec(
		`INSERT INTO alarm_handling_logs (id, alarm_id, action, operator_id, operator_role, note, created_at)
		 VALUES (UUID(), ?, ?, ?, ?, ?, NOW())`,
		alarmID, action, operatorID, operatorRole, note,
	)
}

// ============ 统计数据 ============

func AlarmStats(c *gin.Context) {
	rows, err := database.DB.Query(
		`SELECT alarm_level, status, COUNT(*) as cnt
		 FROM alarm_records WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
		 GROUP BY alarm_level, status`,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}
	defer rows.Close()

	stats := make(map[string]map[string]int)
	for rows.Next() {
		var level, status string
		var cnt int
		rows.Scan(&level, &status, &cnt)
		if stats[level] == nil {
			stats[level] = make(map[string]int)
		}
		stats[level][status] = cnt
	}

	// 各等级总数
	var p0, p1, p2, p3 int
	database.DB.QueryRow(
		`SELECT 
		 SUM(CASE WHEN alarm_level='P0' THEN 1 ELSE 0 END),
		 SUM(CASE WHEN alarm_level='P1' THEN 1 ELSE 0 END),
		 SUM(CASE WHEN alarm_level='P2' THEN 1 ELSE 0 END),
		 SUM(CASE WHEN alarm_level='P3' THEN 1 ELSE 0 END)
		 FROM alarm_records WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)`,
	).Scan(&p0, &p1, &p2, &p3)

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"by_level":  stats,
		"total_p0":  p0,
		"total_p1":  p1,
		"total_p2":  p2,
		"total_p3":  p3,
	}))
}

// ============ 报警规则配置 ============

func ListAlarmRules(c *gin.Context) {
	// 简化版本：返回系统预置规则
	rules := []gin.H{
		{"id": "rule_hr_low", "name": "心率过低", "metric": "heart_rate",
			"threshold": 40, "operator": "<", "level": "P1", "enabled": true},
		{"id": "rule_hr_high", "name": "心率过高", "metric": "heart_rate",
			"threshold": 140, "operator": ">", "level": "P1", "enabled": true},
		{"id": "rule_spo2_low", "name": "血氧过低", "metric": "spo2",
			"threshold": 90, "operator": "<", "level": "P1", "enabled": true},
		{"id": "rule_temp_high", "name": "体温过高", "metric": "temperature",
			"threshold": 38.5, "operator": ">", "level": "P2", "enabled": true},
		{"id": "rule_temp_low", "name": "体温过低", "metric": "temperature",
			"threshold": 35.0, "operator": "<", "level": "P2", "enabled": true},
		{"id": "rule_sos", "name": "SOS求救", "metric": "sos",
			"threshold": 0, "operator": "trigger", "level": "P0", "enabled": true},
		{"id": "rule_fall", "name": "跌倒检测", "metric": "fall",
			"threshold": 0, "operator": "trigger", "level": "P1", "enabled": true},
	}
	c.JSON(http.StatusOK, utils.Success(rules))
}

func UpdateAlarmRule(c *gin.Context) {
	c.JSON(http.StatusOK, utils.Success(gin.H{"message": "规则已更新"}))
}

// HandleAlarmReport POST /api/v1/alarms/report
// 设备端报警数据 HTTP 上报（模拟器调试用，复用 handleAlarmForward 逻辑）
func HandleAlarmReport(c *gin.Context) {
	var rawMap map[string]interface{}
	if err := c.ShouldBindJSON(&rawMap); err != nil {
		c.JSON(http.StatusBadRequest, utils.Error(apperrors.ErrCodeBadRequest, "参数错误"))
		return
	}

	deviceSN, _ := rawMap["device_sn"].(string)
	elderlyID, _ := rawMap["elderly_id"].(string)
	alarmType, _ := rawMap["alarm_type"].(string)

	severity, _ := rawMap["severity"].(string)
	if severity == "" {
		severity, _ = rawMap["level"].(string)
	}
	if severity == "" {
		severity = "P1"
	}
	if alarmType == "" {
		alarmType = "SOS"
	}

	logger.Log.Info("[HTTP] Alarm report received",
		zap.String("device_sn", deviceSN),
		zap.String("alarm_type", alarmType),
		zap.String("severity", severity),
	)

	// elderly_id 为空时通过 device_sn 反查
	if elderlyID == "" && deviceSN != "" {
		err := database.DB.QueryRow(
			"SELECT elderly_id FROM devices WHERE device_sn = ? AND status != 'deleted'",
			deviceSN,
		).Scan(&elderlyID)
		if err != nil {
			_ = database.DB.QueryRow("SELECT id FROM elderly_profiles LIMIT 1").Scan(&elderlyID)
		}
	}

	if elderlyID == "" {
		c.JSON(http.StatusBadRequest, utils.Error(apperrors.ErrCodeBadRequest, "无法识别设备对应的老人"))
		return
	}

	// location JSON
	var locationJSON []byte
	if latVal, latOk := rawMap["latitude"]; latOk {
		if lonVal, lonOk := rawMap["longitude"]; lonOk {
			locationJSON, _ = json.Marshal(map[string]float64{
				"latitude":  toFloat64(latVal),
				"longitude": toFloat64(lonVal),
			})
		}
	}
	if locationJSON == nil {
		locationJSON, _ = json.Marshal(nil)
	}

	snapshotJSON, _ := json.Marshal(map[string]interface{}{
		"heart_rate":  safeGet(rawMap, "heart_rate"),
		"spo2":        safeGet(rawMap, "spo2"),
		"temperature": safeGet(rawMap, "temperature"),
		"battery":     safeGet(rawMap, "battery"),
	})

	id := utils.NewUUID()
	_, err := database.DB.Exec(
		`INSERT INTO alarm_records (id, elderly_id, alarm_type, alarm_level, alarm_source,
		 vital_snapshot_json, location_json, description, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 'device', ?, ?, ?, 'pending', NOW(), NOW())`,
		id, elderlyID, alarmType, severity,
		snapshotJSON, locationJSON, alarmType,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "报警写入失败"))
		return
	}

	ctx := context.Background()
	score := float64(int64(utils.SeverityScore(severity))*1e12 + time.Now().UnixNano())
	redis.Client.ZAdd(ctx, redis.KeyAlarmQueue, goredis.Z{Score: score, Member: id})

	// 🔔 实时推送：HTTP 上报不经过 MQTT，需主动广播到前端（报警页/看板立即刷新）
	redis.PublishWS("new_alarm", gin.H{
		"alarm_id":    id,
		"elderly_id":  elderlyID,
		"device_sn":   deviceSN,
		"alarm_type":  alarmType,
		"severity":    severity,
		"level":       severity,
		"description": alarmType,
		"heart_rate":  safeGet(rawMap, "heart_rate"),
		"spo2":        safeGet(rawMap, "spo2"),
		"temperature": safeGet(rawMap, "temperature"),
		"timestamp":   time.Now().Unix(),
	})
	redis.PublishWS("dashboard_refresh", gin.H{
		"reason":    "new_alarm",
		"severity":  severity,
		"alarm_id":  id,
	})

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"message":  "报警上报成功",
		"alarm_id": id,
		"severity": severity,
	}))
}

// toFloat64 将任意 interface{} 安全转换为 float64（用于经纬度等数值字段）
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f
		}
	}
	return 0
}

// safeGet 从 map 中安全取值（key不存在时返回 nil 而非 panic）
func safeGet(m map[string]interface{}, key string) interface{} {
	if v, ok := m[key]; ok {
		return v
	}
	return nil
}
