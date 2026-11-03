package signin

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"elderly-health-backend/internal/shared/database"
	apperrors "elderly-health-backend/internal/shared/errors"
	"elderly-health-backend/internal/shared/logger"
	"elderly-health-backend/internal/shared/middleware"
	"elderly-health-backend/internal/shared/mqtt"
	"elderly-health-backend/internal/shared/redis"
	"elderly-health-backend/internal/shared/utils"
)

func RegisterRoutes(r *gin.RouterGroup, jwtSecret string) {
	auth := r.Group("")
	auth.Use(middleware.JWTAuth(jwtSecret))
	{
		auth.GET("/signin", ListSignin)
		auth.GET("/signin/overview", SigninOverview)
		auth.GET("/signin/status", GetTodaySigninStatus)
		auth.GET("/signin/status/:elderly_id", GetElderlySigninStatus)
		auth.GET("/signin/:elderly_id/calendar", GetSigninCalendar)
		auth.GET("/signin/community", GetCommunitySignin)
		auth.POST("/signin/proxy", ProxyCheckin)
	}
}

// ============ 定时调度 ============

func DailySigninDispatch() {
	logger.Log.Info("Starting daily signin dispatch (08:00)")

	rows, err := database.DB.Query(
		`SELECT e.id as elderly_id, d.device_sn 
		 FROM elderly_profiles e JOIN devices d ON d.elderly_id = e.id
		 WHERE e.status='active' AND d.status='online'`)
	if err != nil {
		logger.Log.Error("Failed to query elderly for signin", zap.Error(err))
		return
	}
	defer rows.Close()

	for rows.Next() {
		var elderlyID, deviceSN string
		rows.Scan(&elderlyID, &deviceSN)

		cmd := mqtt.SigninPushCommand{
			PushID:      utils.NewUUID(),
			Message:     "早上好！请点击签到，让我们知道您今天安好。",
			Greeting:    "早安问候",
			Priority:    1,
			AutoDismiss: 300,
		}

		if err := mqtt.MQTTClient.Publish(
			mqtt.DeviceTopic(mqtt.TopicDeviceSigninPush, deviceSN), 1, cmd,
		); err != nil {
			logger.Log.Warn("Failed to send signin push",
				zap.String("sn", deviceSN), zap.Error(err))
		}

		// 记录签到提醒
		database.DB.Exec(
			`INSERT INTO signin_records (id, elderly_id, signin_status, signin_date, created_at)
			 VALUES (UUID(), ?, 'pending', CURDATE(), NOW())
			 ON DUPLICATE KEY UPDATE signin_status='pending'`,
			elderlyID,
		)
	}

	logger.Log.Info("Daily signin dispatch completed")
}

func SecondReminder() {
	logger.Log.Info("Starting second signin reminder (08:30)")
	// 查询未签到的老人，发送二次提醒
	rows, err := database.DB.Query(
		`SELECT sr.elderly_id, d.device_sn 
		 FROM signin_records sr JOIN devices d ON d.elderly_id = sr.elderly_id
		 WHERE sr.signin_date = CURDATE() AND sr.signin_status = 'pending' AND d.status='online'`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var elderlyID, deviceSN string
		rows.Scan(&elderlyID, &deviceSN)

		cmd := mqtt.SigninPushCommand{
			PushID:      utils.NewUUID(),
			Message:     "您还没有签到哦，请尽快签到～",
			Greeting:    "签到提醒",
			Priority:    2,
			AutoDismiss: 120,
		}
		mqtt.MQTTClient.Publish(mqtt.DeviceTopic(mqtt.TopicDeviceSigninPush, deviceSN), 1, cmd)
	}
}

func NotifyFamily() {
	logger.Log.Info("Notifying families for unchecked signins (09:00)")
	// TODO: 查询未签到老人，推送通知给绑定子女
}

func PhoneCallCheck() {
	logger.Log.Info("Initiating phone call checks (10:00)")
	// TODO: 查询仍未签到的老人，自动拨打电话
}

// ============ REST 接口 ============

func GetTodaySigninStatus(c *gin.Context) {
	userID, _ := c.Get("user_id")

	// 查询绑定老人的签到状态
	rows, err := database.DB.Query(
		`SELECT e.id, e.name, 
		 COALESCE(sr.signin_status, 'not_started') as signin_status,
		 COALESCE(sr.signin_method, '') as signin_method,
		 sr.signin_at
		 FROM family_bindings fb JOIN elderly_profiles e ON fb.elderly_id = e.id
		 LEFT JOIN signin_records sr ON sr.elderly_id = e.id AND sr.signin_date = CURDATE()
		 WHERE fb.user_id = ? AND e.status = 'active'`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}
	defer rows.Close()

	var list []gin.H
	for rows.Next() {
		var id, name, status, method string
		var signinAt *time.Time
		rows.Scan(&id, &name, &status, &method, &signinAt)
		list = append(list, gin.H{
			"elderly_id": id, "elderly_name": name,
			"signin_status": status, "signin_method": method, "signin_at": signinAt,
		})
	}

	c.JSON(http.StatusOK, utils.Success(list))
}

func GetElderlySigninStatus(c *gin.Context) {
	elderlyID := c.Param("elderly_id")

	// 从 Redis 缓存读取
	ctx := context.Background()
	redisStatus, _ := redis.CacheHelper.Get(ctx, redis.Key(redis.KeySigninToday, elderlyID))

	status := "not_checked_in"
	if redisStatus == "1" {
		status = "checked_in"
	}

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"elderly_id": elderlyID,
		"date":       time.Now().Format("2006-01-02"),
		"status":     status,
	}))
}

func GetSigninCalendar(c *gin.Context) {
	elderlyID := c.Param("elderly_id")
	month := c.Query("month") // 2026-06

	if month == "" {
		month = time.Now().Format("2006-01")
	}

	rows, err := database.DB.Query(
		`SELECT signin_date, signin_status, signin_method, signin_at
		 FROM signin_records 
		 WHERE elderly_id = ? AND DATE_FORMAT(signin_date, '%Y-%m') = ?
		 ORDER BY signin_date`, elderlyID, month,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}
	defer rows.Close()

	calendar := make(map[string]gin.H)
	for rows.Next() {
		var date, status, method string
		var signinAt time.Time
		rows.Scan(&date, &status, &method, &signinAt)
		calendar[date] = gin.H{
			"date": date, "status": status,
			"method": method, "time": signinAt.Format("15:04"),
		}
	}

	// 计算签到率
	var total, checkedIn int
	database.DB.QueryRow(
		`SELECT COUNT(*), SUM(CASE WHEN signin_status='checked_in' THEN 1 ELSE 0 END)
		 FROM signin_records WHERE elderly_id=? AND DATE_FORMAT(signin_date,'%Y-%m')=?`,
		elderlyID, month,
	).Scan(&total, &checkedIn)

	rate := float64(0)
	if total > 0 {
		rate = float64(checkedIn) / float64(total) * 100
	}

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"calendar": calendar,
		"total_days": total,
		"checked_in": checkedIn,
		"signin_rate": rate,
	}))
}

func GetCommunitySignin(c *gin.Context) {
	communityID := c.Query("community_id")

	where := "WHERE e.status = 'active'"
	var args []interface{}
	if communityID != "" {
		where += " AND e.community_id = ?"
		args = append(args, communityID)
	}

	rows, err := database.DB.Query(
		`SELECT id, name, status, method
		 FROM (
			SELECT
				e.id as id,
				e.name as name,
				CASE
					WHEN sr.signin_status = 'checked_in' THEN 'signed'
					WHEN sr.signin_status IN ('pending', 'not_started') OR sr.signin_status IS NULL THEN 'unsigned'
					ELSE 'timeout'
				END as status,
				COALESCE(sr.signin_method, '') as method
			FROM elderly_profiles e
			LEFT JOIN signin_records sr ON sr.elderly_id = e.id AND sr.signin_date = CURDATE()
			`+where+`
		 ) t
		 ORDER BY FIELD(status, 'unsigned', 'timeout', 'signed'), name`,
		args...,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}
	defer rows.Close()

	var total, signed, unsigned, timeout int
	var list []gin.H
	for rows.Next() {
		var id, name, status, method string
		rows.Scan(&id, &name, &status, &method)
		list = append(list, gin.H{
			"elderly_id": id, "name": name, "status": status, "method": method,
		})
		total++
		switch status {
		case "signed":
			signed++
		case "unsigned":
			unsigned++
		case "timeout":
			timeout++
		}
	}

	rate := float64(0)
	if total > 0 {
		rate = float64(signed) / float64(total) * 100
	}

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"list": list,
		"summary": gin.H{
			"total":    total,
			"signed":   signed,
			"unsigned": unsigned,
			"timeout":  timeout,
			"rate":     rate,
		},
	}))
}

func ListSignin(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	elderlyID := c.Query("elderly_id")
	offset := (page - 1) * pageSize

	where := "WHERE e.status = 'active'"
	var filterArgs []interface{}
	if elderlyID != "" {
		where += " AND e.id = ?"
		filterArgs = append(filterArgs, elderlyID)
	}

	var total int64
	if err := database.DB.QueryRow(
		"SELECT COUNT(*) FROM elderly_profiles e "+
			"LEFT JOIN signin_records sr ON sr.elderly_id = e.id AND sr.signin_date = CURDATE() "+
			where,
		filterArgs...,
	).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}

	listArgs := append(filterArgs, offset, pageSize)
	rows, err := database.DB.Query(
		`SELECT id, elderly_id, elderly_name, signin_date, signin_time, method, status, proxy_by
		 FROM (
			SELECT
				COALESCE(sr.id, e.id) as id,
				e.id as elderly_id,
				e.name as elderly_name,
				COALESCE(sr.signin_date, CURDATE()) as signin_date,
				sr.signin_at as signin_time,
				COALESCE(sr.signin_method, '') as method,
				CASE
					WHEN sr.signin_status = 'checked_in' THEN 'signed'
					WHEN sr.signin_status IN ('pending', 'not_started') OR sr.signin_status IS NULL THEN 'unsigned'
					ELSE 'timeout'
				END as status,
				sr.proxy_by
			FROM elderly_profiles e
			LEFT JOIN signin_records sr ON sr.elderly_id = e.id AND sr.signin_date = CURDATE()
			`+where+`
		 ) t
		 ORDER BY FIELD(status, 'unsigned', 'timeout', 'signed'), elderly_name
		 LIMIT ?, ?`,
		listArgs...,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}
	defer rows.Close()

	var list []gin.H
	for rows.Next() {
		var id, elderlyID, elderlyName, signinDate, method, status string
		var signinAt *time.Time
		var proxyBy sql.NullString
		rows.Scan(&id, &elderlyID, &elderlyName, &signinDate, &signinAt, &method, &status, &proxyBy)
		list = append(list, gin.H{
			"id":           id,
			"elderly_id":   elderlyID,
			"elderly_name": elderlyName,
			"signin_date":  signinDate,
			"signin_time":  signinAt,
			"method":       method,
			"status":       status,
			"proxy_by":     proxyBy.String,
		})
	}

	c.JSON(http.StatusOK, utils.SuccessPage(list, total, page, pageSize))
}

func SigninOverview(c *gin.Context) {
	var total, signed, unsigned, timeout int64
	err := database.DB.QueryRow(
		`SELECT COUNT(e.id),
		 COALESCE(SUM(CASE WHEN COALESCE(sr.signin_status,'not_started')='checked_in' THEN 1 ELSE 0 END),0),
		 COALESCE(SUM(CASE WHEN COALESCE(sr.signin_status,'not_started') IN ('pending','not_started') THEN 1 ELSE 0 END),0),
		 COALESCE(SUM(CASE WHEN COALESCE(sr.signin_status,'not_started') IN ('late','absent') THEN 1 ELSE 0 END),0)
		 FROM elderly_profiles e
		 LEFT JOIN signin_records sr ON sr.elderly_id = e.id AND sr.signin_date = CURDATE()
		 WHERE e.status = 'active'`,
	).Scan(&total, &signed, &unsigned, &timeout)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "查询失败"))
		return
	}

	rate := float64(0)
	if total > 0 {
		rate = float64(signed) / float64(total) * 100
	}

	c.JSON(http.StatusOK, utils.Success(gin.H{
		"total":     total,
		"signed":    signed,
		"unsigned":  unsigned,
		"timeout":   timeout,
		"rate":      rate,
	}))
}

type ProxyCheckinRequest struct {
	ElderlyID string `json:"elderly_id" binding:"required"`
	Note      string `json:"note"`
}

func ProxyCheckin(c *gin.Context) {
	var req ProxyCheckinRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.Error(apperrors.ErrCodeBadRequest, "参数错误"))
		return
	}

	operatorID, _ := c.Get("user_id")

	_, err := database.DB.Exec(
		`INSERT INTO signin_records (id, elderly_id, signin_method, signin_status, 
		 proxy_by, signin_at, signin_date, created_at)
		 VALUES (UUID(), ?, 'proxy', 'checked_in', ?, NOW(), CURDATE(), NOW())
		 ON DUPLICATE KEY UPDATE signin_method='proxy', signin_status='checked_in',
		 proxy_by=?, signin_at=NOW()`,
		req.ElderlyID, operatorID, operatorID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.Error(apperrors.ErrCodeInternal, "代签失败"))
		return
	}

	// 更新缓存
	ctx := context.Background()
	redis.CacheHelper.Set(ctx, redis.Key(redis.KeySigninToday, req.ElderlyID), "1", 24*time.Hour)

	// 🔔 实时推送：签到成功后通知前端刷新签到列表/看板签到率
	redis.PublishWS("signin_notify", gin.H{
		"elderly_id":  req.ElderlyID,
		"signin_time": time.Now().Format(time.RFC3339),
		"method":      "proxy",
	})
	redis.PublishWS("dashboard_refresh", gin.H{
		"reason":     "signin",
		"elderly_id": req.ElderlyID,
	})

	c.JSON(http.StatusOK, utils.Success(gin.H{"message": "代签成功"}))
}
