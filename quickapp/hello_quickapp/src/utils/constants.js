/**
 * 常量配置
 * 服务器地址：http://101.35.231.154/api/v1
 */

// 体征正常范围
export const VITAL_RANGES = {
  heartRate: {
    min: 60,
    max: 100,
    warningLow: 50,
    warningHigh: 120,
    dangerLow: 30,
    dangerHigh: 180
  },
  spo2: {
    min: 95,
    max: 100,
    warning: 92,
    danger: 85
  },
  temperature: {
    min: 36.1,
    max: 37.2,
    warningLow: 35.5,
    warningHigh: 37.5
  }
}

// 活动目标默认值
export const ACTIVITY_DEFAULTS = {
  goalSteps: 6000,
  collectInterval: 5,
  uploadInterval: 5
}

// 体征数据上报间隔（毫秒）：每 5 秒上报一次
export const VITAL_UPLOAD_INTERVAL = 5000

// 设备绑定
// 出厂默认序列号；可在「设置 → 设备绑定」修改，
// 后续接入二维码扫码绑定时，手机端生成的二维码内容为：
//   ehc://bind?sn=<device_sn>&elderly_id=<elderly_id>
export const DEVICE_BINDING = {
  DEFAULT_SN: 'EH-WATCH-20260101',
  DEFAULT_ELDERLY_ID: '47e306a7-76b0-11f1-a296-0242ac130012',
  QR_SCHEME: 'ehc://bind'
}

// 报警级别
export const ALARM_LEVELS = {
  P0: 'P0',
  P1: 'P1',
  P2: 'P2',
  P3: 'P3'
}

// 健康提示级别
export const TIP_LEVELS = {
  INFO: 'info',
  WARNING: 'warning',
  DANGER: 'danger'
}

// 存储键名
export const STORAGE_KEYS = {
  USER_SETTINGS: 'user_settings',
  TODAY_ACTIVITY: 'today_activity',
  SOS_RECORDS: 'sos_records',
  SOS_CACHE: 'sos_cache',
  // 待补传的报警（发送失败后持久化，网络恢复自动重发）
  ALARM_PENDING: 'alarm_pending',
  VITAL_HISTORY: 'vital_history',
  VITAL_CACHE: 'vital_cache',
  UPLOAD_LOG: 'upload_log',
  UPLOAD_MODE: 'upload_mode',
  MQTT_STATUS: 'mqtt_status',
  MQTT_CMD: 'mqtt_cmd',
  CHECKIN_DATE: 'checkin_date'
}

// ============================================
// 服务器 API 配置
//
// 网关入口：http://101.35.231.154/api/v1
//   线上网关已被 Caddy 308 重定向到 HTTPS（裸 IP 无证书），
//   因此默认改为「按微服务直连」，端口与网关后的服务一一对应。
// ============================================
export const API_CONFIG = {
  BASE_URL: 'http://101.35.231.154',
  VERSION: '/api/v1',

  // 微服务直连地址（当前线上可用）
  SERVICES: {
    AUTH: 'http://101.35.231.154:8011',   // admin-auth-service
    USER: 'http://101.35.231.154:8001',   // user-service
    DEVICE: 'http://101.35.231.154:8002', // device-service
    VITAL: 'http://101.35.231.154:8003',  // vital-service
    ALARM: 'http://101.35.231.154:8004',  // alarm-service
    SIGNIN: 'http://101.35.231.154:8005'  // signin-service
  },

  // WebSocket 长连接候选地址（按顺序尝试，失败自动切换）
  //   /ws           → Caddy 网关（网关修复后优先可用）
  //   :8012/ws      → websocket-service 直连
  WS_URLS: [
    'ws://101.35.231.154/ws',
    'ws://101.35.231.154:8012/ws'
  ],
  WS_URL: 'ws://101.35.231.154/ws',

  // 登录账号（可被 storage 中的 AUTH_CREDENTIALS 覆盖）
  AUTH: {
    USERNAME: 'admin',
    PASSWORD: 'Admin@2026',
    LOGIN: '/v1/admin/login',
    REFRESH: '/v1/admin/refresh',
    LOGOUT: '/v1/admin/logout'
  },

  // WebSocket 连接参数
  WS: {
    PING_INTERVAL: 25000,
    CONNECT_TIMEOUT: 10000,
    RECONNECT_BASE: 2000,
    RECONNECT_MAX: 60000,
    SUBSCRIBE_TOPICS: ['global', 'devices', 'alarms', 'vitals', 'signins']
  },

  // 设备信息（根据实际设备填写）
  DEVICE_SN: 'EH-WATCH-20260101',
  ELDERLY_ID: '47e306a7-76b0-11f1-a296-0242ac130012',

  // MQTT 配置（真机使用）
  MQTT: {
    // TCP Broker（原生 MQTT，需要平台 socket 支持）
    BROKER: 'mqtt://101.35.231.154:1883',
    // WebSocket Broker（MQTT over WebSocket，手表平台推荐）
    WS_BROKER: 'ws://101.35.231.154:8083/mqtt',
    // 登录凭证
    USERNAME: 'elderly_device',
    PASSWORD: 'SilverHair@2026',
    CLIENT_ID: 'EH-WATCH-20260101',
    // 连接参数
    KEEP_ALIVE: 60,
    QOS: 1,
    CLEAN_START: false,
    SESSION_EXPIRY: 86400
  },

  // HTTP API 端点（模拟器调试使用）
  ENDPOINTS: {
    // 体征数据上报（模拟器通过HTTP POST）
    VITAL_SIGN: '/api/v1/vitals/sign',
    // 报警数据上报（模拟器通过HTTP POST）
    ALARM_REPORT: '/api/v1/alarms/report',
    // 设备心跳
    DEVICE_HEARTBEAT: '/api/v1/devices/heartbeat',
    // 设备上线
    DEVICE_ONLINE: '/api/v1/devices/online',
    // 设备位置上报
    DEVICE_POSITION: '/api/v1/devices/position',
    // 查询体征历史
    VITAL_HISTORY: '/api/v1/vitals',
    // 查询报警列表
    ALARM_LIST: '/api/v1/alarms',

    // 微服务直连路径（配合 API_CONFIG.SERVICES 使用）
    V1: {
      VITAL_SIGN: '/v1/vitals/sign',
      ALARM_REPORT: '/v1/alarms/report',
      DEVICE_HEARTBEAT: '/v1/devices/heartbeat',
      DEVICE_ONLINE: '/v1/devices/online',
      DEVICE_POSITION: '/v1/devices/position',
      DEVICE_LIST: '/v1/devices',
      // 代签到：设备端签到写入 signin_records
      SIGNIN_PROXY: '/v1/signin/proxy',
      // 今日签到状态
      SIGNIN_STATUS: '/v1/signin/status'
    }
  },

  // MQTT Topic 定义
  MQTT_TOPICS: {
    VITAL: (sn) => `device/${sn}/vital`,
    ALARM: (sn) => `device/${sn}/alarm`,
    HEARTBEAT: (sn) => `device/${sn}/heartbeat`,
    ONLINE: (sn) => `device/${sn}/online`,
    POSITION: (sn) => `device/${sn}/position`,
    BATCH: (sn) => `device/${sn}/batch`,
    SIGNIN: (sn) => `device/${sn}/signin`
  }
}

// 传感器配置
export const SENSOR_CONFIG = {
  heartRateInterval: 1000,
  spo2Interval: 5000,
  accelerometerInterval: 100
}

// 上传模式
export const UPLOAD_MODE = {
  HTTP: 'http',   // 模拟器调试：通过HTTP API上传
  MQTT: 'mqtt'    // 真机：通过MQTT上传
}

// 上传模式的中文标签
export const UPLOAD_MODE_LABELS = {
  http: 'HTTP 模式（调试）',
  mqtt: 'MQTT 模式（真机）'
}
