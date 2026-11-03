/**
 * MQTT 客户端服务 —— OpenVela / QuickApp 适配版
 *
 * OpenVela 模拟器分析：
 *   - `@system.websocket` 原生支持缺失
 *   - 没有全局 `WebSocket`
 *   - 只有 `@system.fetch` 可用
 *
 * 解决方案：
 *   使用 `@system.fetch` 发送 HTTP POST 到服务端
 *   服务端收到后作为 MQTT 消息转发
 */

import { API_CONFIG } from '../utils/constants'
import fetch from '@system.fetch'
import deviceBinding from './DeviceBindingService'

// 传输层类型
const TRANSPORT_NONE = 0
const TRANSPORT_HTTP = 1  // @system.fetch

export class MqttService {
  constructor() {
    this.connected = false
    this.connecting = false
    this._transportType = TRANSPORT_NONE

    // 连接配置
    this.broker = API_CONFIG.MQTT.WS_BROKER
    this.username = API_CONFIG.MQTT.USERNAME
    this.password = API_CONFIG.MQTT.PASSWORD
    this.clientId = API_CONFIG.MQTT.CLIENT_ID
    this.keepAlive = API_CONFIG.MQTT.KEEP_ALIVE

    // 重连配置
    this.reconnectTimer = null
    this.reconnectDelay = 2000
    this.maxReconnectDelay = 60000
    this.reconnectAttempts = 0
    this.reconnectEnabled = true

    // 心跳
    this.pingTimer = null

    // 离线缓存
    this.pendingPublishes = []

    // 回调
    this.onStatusChange = null
    this.onMessage = null
    this.onError = null

    // 电量（外部通过 setBatteryLevel 更新）
    this._batteryLevel = 100
  }

  /**
   * 初始化 MQTT 服务
   */
  init(options = {}) {
    this.onStatusChange = options.onStatusChange || null
    this.onMessage = options.onMessage || null
    this.onError = options.onError || null

    const transportName = this._detectTransport()
    console.log(`[MQTT] 初始化完成, 传输层: ${transportName}`)
    console.log(`[MQTT] Broker: ${API_CONFIG.BASE_URL}`)
    console.log(`[MQTT] ClientID: ${this.clientId}`)
  }

  /**
   * 检测可用的传输层
   */
  _detectTransport() {
    // OpenVela 模拟器只有 HTTP 可用
    this._transportType = TRANSPORT_HTTP
    return '@system.fetch (HTTP)'
  }

  /**
   * 建立连接（发送上线消息）
   */
  connect() {
    if (this.connected || this.connecting) {
      console.log('[MQTT] 已连接/连接中，跳过')
      return
    }

    if (this._transportType !== TRANSPORT_HTTP) {
      console.error('[MQTT] 无可用传输层')
      this._notifyError('平台不支持 MQTT 连接')
      return
    }

    this.connecting = true
    this._notifyStatus(false, 'connecting')
    console.log(`[MQTT] 正在连接: ${API_CONFIG.BASE_URL}`)

    // 发送上线消息（字段匹配服务端 HandleHTTPDeviceOnline）
    const packet = {
      device_sn: deviceBinding.getDeviceSN(),
      elderly_id: deviceBinding.getElderlyId(),
      firmware_version: '1.0.0'
    }

    this._sendHttpRequest(
      `${API_CONFIG.SERVICES.DEVICE}${API_CONFIG.ENDPOINTS.V1.DEVICE_ONLINE}`,
      packet,
      () => {
        // 成功：标记为已连接
        this.connected = true
        this.connecting = false
        this.reconnectAttempts = 0
        console.log('[MQTT] ★ 连接成功（HTTP 模式）')
        this._notifyStatus(true, 'connected (HTTP)')
        this._startPing()
        this._flushPendingPublishes()
      },
      (error) => {
        // 失败
        console.error(`[MQTT] 连接失败: ${error}`)
        this.connecting = false
        this._notifyError(`HTTP 连接失败: ${error}`)
        this._scheduleReconnect()
      }
    )
  }

  /**
   * 断开连接
   */
  disconnect() {
    this.reconnectEnabled = false
    this._clearReconnect()
    this._stopPing()

    this.connected = false
    this.connecting = false
    this._notifyStatus(false, 'disconnected')
    console.log('[MQTT] 已断开')
  }

  /**
   * 发布消息
   */
  publish(topic, payload, options = {}) {
    const qos = options.qos !== undefined ? options.qos : API_CONFIG.MQTT.QOS
    const retain = options.retain || false

    if (!this.connected) {
      // options.cache === false 时不入缓存（调用方会立即回退 HTTP，避免重复发送）
      if (options.cache !== false) {
        console.log(`[MQTT] 未连接，缓存: ${topic}`)
        this.pendingPublishes.push({ topic, payload, options })
      }
      if (!this.connecting && this.reconnectEnabled) this.connect()
      return false
    }

    // HTTP 模式：直接 POST 到服务器
    console.log(`[MQTT] 发布: ${topic}`)

    // 根据 topic 选择正确的 API 端点（微服务直连）
    let url = ''
    if (topic.includes('/vital')) {
      url = API_CONFIG.SERVICES.VITAL + API_CONFIG.ENDPOINTS.V1.VITAL_SIGN
    } else if (topic.includes('/alarm')) {
      url = API_CONFIG.SERVICES.ALARM + API_CONFIG.ENDPOINTS.V1.ALARM_REPORT
    } else if (topic.includes('/heartbeat')) {
      url = API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_HEARTBEAT
    } else if (topic.includes('/position')) {
      url = API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_POSITION
    } else if (topic.includes('/online')) {
      url = API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_ONLINE
    } else {
      url = API_CONFIG.BASE_URL + `/api/v1/${topic}`
    }

    // options.onSuccess / options.onError：发布结果回调（调用方可据此回退 HTTP）
    this._sendHttpRequest(url, payload, options.onSuccess, options.onError)
    return true
  }

  /**
   * 获取连接状态
   */
  isConnected() { return this.connected }

  /**
   * 获取详细状态
   */
  getStatus() {
    return {
      connected: this.connected,
      connecting: this.connecting,
      transport: '@system.fetch (HTTP)',
      broker: API_CONFIG.BASE_URL,
      clientId: this.clientId,
      reconnectAttempts: this.reconnectAttempts
    }
  }

  /**
   * 销毁服务
   */
  destroy() {
    this.disconnect()
    this.pendingPublishes = []
    this.onStatusChange = null
    this.onMessage = null
    this.onError = null
  }

  // ============================================
  // HTTP 传输层
  // ============================================

  /**
   * 发送 HTTP POST 请求
   */
  _sendHttpRequest(url, data, successCallback, errorCallback) {
    fetch.fetch({
      url: url,
      method: 'POST',
      header: {
        'Content-Type': 'application/json'
      },
      data: JSON.stringify(data),
      responseType: 'json',
      success: (response) => {
        const res = response.data || response
        const code = res.code !== undefined ? res.code : response.code
        if (code === 0 || code === 200) {
          console.log(`[MQTT-HTTP] 成功: ${url}`)
          if (successCallback) successCallback(res)
        } else {
          console.error(`[MQTT-HTTP] 业务失败: code=${code} url=${url}`)
          if (errorCallback) errorCallback(`code=${code}`)
        }
      },
      fail: (data, code) => {
        console.error(`[MQTT-HTTP] 请求失败: code=${code} url=${url}`)
        if (errorCallback) errorCallback(`code=${code}`)
      }
    })
  }

  // ============================================
  // 连接管理
  // ============================================

  _scheduleReconnect() {
    if (!this.reconnectEnabled || this.connected || this.connecting) return
    this._clearReconnect()

    this.reconnectAttempts++
    const delay = Math.min(
      this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
      this.maxReconnectDelay
    )

    console.log(`[MQTT] ${delay / 1000}s 后重连 (第 ${this.reconnectAttempts} 次)`)
    this._notifyStatus(false, `reconnecting:${this.reconnectAttempts}`)

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
  }

  _clearReconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  // ============================================
  // 心跳
  // ============================================

  _startPing() {
    this._stopPing()
    const interval = Math.max(this.keepAlive * 1000 * 0.8, 10000)
    this.pingTimer = setInterval(() => {
      if (this.connected) this._sendPing()
    }, interval)
    console.log(`[MQTT] PING 间隔: ${interval}ms`)
  }

  _stopPing() {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null }
  }

  _sendPing() {
    // 心跳：字段匹配服务端 HandleHTTPHeartbeat
    const url = `${API_CONFIG.SERVICES.DEVICE}${API_CONFIG.ENDPOINTS.V1.DEVICE_HEARTBEAT}`
    this._sendHttpRequest(url, {
      device_sn: deviceBinding.getDeviceSN(),
      battery: this._batteryLevel || 100,
      signal_strength: 72,
      firmware_version: '1.0.0'
    }, () => {}, () => {})
  }

  /**
   * 更新电量（由外部调用，用于心跳上报真实电量）
   */
  setBatteryLevel(level) {
    this._batteryLevel = level
  }

  // ============================================
  // 缓存发送
  // ============================================

  _flushPendingPublishes() {
    if (!this.pendingPublishes.length) return
    console.log(`[MQTT] 发送 ${this.pendingPublishes.length} 条缓存`)
    const pending = [...this.pendingPublishes]
    this.pendingPublishes = []
    pending.forEach(item => this.publish(item.topic, item.payload, item.options))
  }

  // ============================================
  // 回调
  // ============================================

  _notifyStatus(connected, info) {
    if (this.onStatusChange) this.onStatusChange(connected, info)
  }

  _notifyError(error) {
    if (this.onError) this.onError(error)
  }
}

export default MqttService
