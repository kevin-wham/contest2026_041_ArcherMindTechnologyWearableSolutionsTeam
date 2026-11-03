/**
 * WebSocket 长连接服务（快应用手表平台：@system.websocketfactory）
 *
 * 职责：
 *   1. 使用登录得到的 access_token 建立 WebSocket 连接
 *   2. 连接成功后发送订阅消息，并定时发送 ping 保活
 *   3. 断线自动重连（指数退避），支持多个候选地址自动降级
 *   4. 向服务器下发指令（如报警指令），并接收服务器推送
 *
 * 说明：服务器端 /ws 目前只处理 ping 与消息推送，
 *       报警数据同时通过 HTTP /v1/alarms/report 写入数据库，保证不丢。
 */
import { API_CONFIG } from '../utils/constants'

/**
 * 运行时加载原生 WebSocket 模块。
 * 平台不支持时返回 null，保证应用其它功能不受影响。
 */
function loadWebSocketFactory() {
  const moduleNames = ['@system.websocketfactory']
  for (let i = 0; i < moduleNames.length; i++) {
    try {
      const name = moduleNames[i]
      const factory = require(name)
      if (factory && typeof factory.create === 'function') {
        return factory
      }
    } catch (e) {
      console.warn('[WS] 加载 ' + moduleNames[i] + ' 失败: ' + e)
    }
  }
  return null
}

export class WebSocketService {
  constructor() {
    this.factory = undefined // undefined 表示尚未检测
    this.ws = null

    this.connected = false
    this.connecting = false
    this.status = 'idle'

    this.tokenProvider = null
    this.onStatusChange = null
    this.onMessage = null
    this.onError = null
    this.onNeedToken = null

    this.manualClosed = false
    this.reconnectEnabled = true
    this.reconnectAttempts = 0
    this.reconnectTimer = null
    this.pingTimer = null
    this.connectTimer = null

    this.urls = (API_CONFIG.WS_URLS || [API_CONFIG.WS_URL]).slice()
    this.urlIndex = 0
    this.currentUrl = ''
  }

  /**
   * @param {Object} options { tokenProvider, onStatusChange, onMessage, onError, onNeedToken }
   */
  init(options = {}) {
    this.tokenProvider = options.tokenProvider || null
    this.onStatusChange = options.onStatusChange || null
    this.onMessage = options.onMessage || null
    this.onError = options.onError || null
    this.onNeedToken = options.onNeedToken || null
  }

  isSupported() {
    if (this.factory === undefined) {
      this.factory = loadWebSocketFactory()
      console.log('[WS] 平台 WebSocket 支持: ' + (this.factory ? '是' : '否'))
    }
    return !!this.factory
  }

  isConnected() {
    return this.connected
  }

  isConnecting() {
    return this.connecting
  }

  getStatus() {
    return {
      supported: this.isSupported(),
      connected: this.connected,
      connecting: this.connecting,
      status: this.status,
      url: this.currentUrl,
      attempts: this.reconnectAttempts
    }
  }

  /**
   * 建立连接
   * @param {String} token access_token
   */
  connect(token) {
    if (this.connected || this.connecting) return

    if (!this.isSupported()) {
      this._setStatus('unsupported')
      return
    }

    if (!token) {
      console.warn('[WS] 缺少 token，无法建立连接')
      this._setStatus('no_token')
      if (this.onNeedToken) this.onNeedToken()
      return
    }

    const base = this.urls[this.urlIndex % this.urls.length]
    const url = base + (base.indexOf('?') >= 0 ? '&' : '?') + 'token=' + encodeURIComponent(token)

    this.connecting = true
    this.manualClosed = false
    this.reconnectEnabled = true
    this.currentUrl = url
    this._setStatus('connecting')
    console.log('[WS] 正在连接: ' + base)

    try {
      this.ws = this.factory.create({
        url: url,
        header: { 'Content-Type': 'application/json' }
      })
    } catch (e) {
      console.error('[WS] 创建连接失败: ' + e)
      this.connecting = false
      this.ws = null
      this._setStatus('create_failed')
      this._nextUrl()
      this._scheduleReconnect()
      return
    }

    this.ws.onopen = () => this._handleOpen()
    this.ws.onmessage = (data) => this._handleMessage(data)
    this.ws.onclose = (data) => this._handleClose(data)
    this.ws.onerror = (data) => this._handleError(data)

    // 握手超时保护（部分平台握手失败时不会触发 onclose）
    this.connectTimer = setTimeout(() => {
      this.connectTimer = null
      if (this.connected) return
      console.warn('[WS] 连接超时，切换候选地址')
      this._closeSocket()
      this.connecting = false
      this._setStatus('timeout')
      this._nextUrl()
      this._scheduleReconnect()
    }, API_CONFIG.WS.CONNECT_TIMEOUT)
  }

  /**
   * 主动断开（不再自动重连）
   */
  disconnect() {
    this.manualClosed = true
    this.reconnectEnabled = false
    this._clearTimers()
    this._closeSocket()
    this.connected = false
    this.connecting = false
    this._setStatus('disconnected')
  }

  /**
   * 销毁服务
   */
  destroy() {
    this.disconnect()
    this.onStatusChange = null
    this.onMessage = null
    this.onError = null
    this.onNeedToken = null
    this.tokenProvider = null
  }

  /**
   * 发送数据
   * @param {Object|String} payload
   * @return {Boolean} 是否已交给底层发送
   */
  send(payload) {
    if (!this.connected || !this.ws) return false

    const data = typeof payload === 'string' ? payload : JSON.stringify(payload)
    try {
      this.ws.send({
        data: data,
        fail: (res, code) => console.warn('[WS] 发送失败: code=' + code)
      })
      return true
    } catch (e) {
      console.warn('[WS] 发送异常: ' + e)
      return false
    }
  }

  /**
   * 下发报警指令
   */
  sendAlarm(alarmPacket) {
    const ok = this.send({
      type: 'alarm_report',
      timestamp: Date.now(),
      data: alarmPacket
    })
    if (ok) {
      console.log('[WS] 报警指令已下发: ' + alarmPacket.alarm_type + '/' + alarmPacket.severity)
    }
    return ok
  }

  // ============================================
  // 事件处理
  // ============================================

  _handleOpen() {
    if (this.connectTimer) {
      clearTimeout(this.connectTimer)
      this.connectTimer = null
    }

    this.connected = true
    this.connecting = false
    this.reconnectAttempts = 0
    console.log('[WS] 连接成功: ' + this.currentUrl)
    this._setStatus('connected')

    this._subscribe()
    this._startPing()
  }

  _handleMessage(event) {
    const raw = event && event.data !== undefined ? event.data : event
    if (typeof raw !== 'string') {
      console.log('[WS] 收到非文本消息，忽略')
      return
    }

    // 服务端可能把多条消息用 \n 拼接后一起下发
    const frames = raw.split('\n')
    for (let i = 0; i < frames.length; i++) {
      const text = frames[i]
      if (!text) continue

      let msg = null
      try {
        msg = JSON.parse(text)
      } catch (e) {
        console.warn('[WS] 消息解析失败: ' + text.substring(0, 60))
        continue
      }

      if (!msg) continue
      if (msg.type === 'pong') continue

      if (msg.type === 'connected') {
        console.log('[WS] 服务端确认连接: ' + JSON.stringify(msg.data || {}))
        continue
      }

      if (msg.type === 'new_alarm') {
        console.log('[WS] 收到新报警推送')
      }

      if (this.onMessage) {
        try {
          this.onMessage(msg)
        } catch (e) {
          console.error('[WS] 消息回调异常: ' + e)
        }
      }
    }
  }

  _handleClose(data) {
    const wasConnected = this.connected
    this.connected = false
    this.connecting = false
    this._clearTimers()
    this.ws = null

    if (wasConnected) {
      console.warn('[WS] 连接已断开: code=' + (data && data.code) + ' reason=' + (data && data.reason))
    }

    this._setStatus('closed')
    if (!this.manualClosed && this.reconnectEnabled) this._scheduleReconnect()
  }

  _handleError(data) {
    console.warn('[WS] 连接异常: ' + ((data && data.data) || 'unknown'))
    this._setStatus('error')
    if (this.onError) {
      try {
        this.onError(data)
      } catch (e) {}
    }
  }

  // ============================================
  // 内部方法
  // ============================================

  _subscribe() {
    this.send({
      action: 'subscribe',
      topics: API_CONFIG.WS.SUBSCRIBE_TOPICS
    })
  }

  _startPing() {
    this._stopPing()
    this.pingTimer = setInterval(() => {
      if (this.connected) this.send({ type: 'ping', timestamp: Date.now() })
    }, API_CONFIG.WS.PING_INTERVAL)
  }

  _stopPing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
  }

  _closeSocket() {
    if (!this.ws) return
    try {
      this.ws.onopen = null
      this.ws.onmessage = null
      this.ws.onclose = null
      this.ws.onerror = null
      this.ws.close({ code: 1000, reason: 'client close' })
    } catch (e) {}
    this.ws = null
  }

  _clearTimers() {
    if (this.connectTimer) {
      clearTimeout(this.connectTimer)
      this.connectTimer = null
    }
    this._stopPing()
  }

  _nextUrl() {
    if (this.urls.length > 1) {
      this.urlIndex = (this.urlIndex + 1) % this.urls.length
      console.log('[WS] 切换候选地址: ' + this.urls[this.urlIndex])
    }
  }

  _scheduleReconnect() {
    if (!this.reconnectEnabled || this.manualClosed || this.reconnectTimer) return

    this.reconnectAttempts++
    const base = API_CONFIG.WS.RECONNECT_BASE
    const exp = Math.min(this.reconnectAttempts - 1, 5)
    const delay = Math.min(base * Math.pow(2, exp), API_CONFIG.WS.RECONNECT_MAX)

    this._setStatus('reconnecting:' + this.reconnectAttempts)
    console.log('[WS] ' + Math.round(delay / 1000) + 's 后重连（第 ' + this.reconnectAttempts + ' 次）')

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      const token = this.tokenProvider ? this.tokenProvider() : ''
      if (!token) {
        this._setStatus('no_token')
        if (this.onNeedToken) this.onNeedToken()
        return
      }
      this.connect(token)
    }, delay)
  }

  _setStatus(status) {
    this.status = status
    if (this.onStatusChange) {
      try {
        this.onStatusChange(this.connected, status)
      } catch (e) {
        console.error('[WS] 状态回调异常: ' + e)
      }
    }
  }
}

export default WebSocketService
