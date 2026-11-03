/**
 * 云端连接服务（单例）
 *
 * 把「登录 → 建立 WebSocket → 下发/接收指令」串成一条链路，
 * 供首页、SOS 页、设置页共享同一个连接（避免重复登录/重复建链）。
 *
 * 对外能力：
 *   cloudService.start()              // 启动：自动登录 + 建立长连接
 *   cloudService.ensureOnline(cb)     // 确保在线（失败自动重试）
 *   cloudService.reportAlarm(packet)  // 通过长连接下发报警指令
 *   cloudService.getStatus()          // 登录/连接状态
 *   cloudService.onStatusChange(fn)   // 订阅状态变化
 */
import storage from '@system.storage'
import AuthService from './AuthService'
import WebSocketService from './WebSocketService'
import { STORAGE_KEYS } from '../utils/constants'

export class CloudService {
  constructor() {
    this.auth = new AuthService()
    this.ws = new WebSocketService()

    this.initialized = false
    this.ensuring = false
    this.ensureWaiters = []

    this.listeners = []
    this.retryTimer = null
    this.retryDelay = 10000
    this.maxRetryDelay = 60000

    this.onAlarmMessage = null

    this.state = {
      loggedIn: false,
      wsConnected: false,
      wsStatus: 'idle',
      username: '',
      lastError: '',
      updatedAt: 0
    }
  }

  /**
   * @param {Object} options { onAlarmMessage(alarmData) }
   */
  init(options = {}) {
    if (options.onAlarmMessage) this.onAlarmMessage = options.onAlarmMessage
    if (this.initialized) return
    this.initialized = true

    this.auth.init({
      onStatusChange: (loggedIn, message) => {
        this.state.loggedIn = loggedIn
        this.state.username = this.auth.getUsername()
        if (loggedIn) {
          this.state.lastError = ''
        } else if (message) {
          this.state.lastError = message
        }
        this._emit()
        if (loggedIn) this._connectWS()
      }
    })

    this.ws.init({
      tokenProvider: () => (this.auth.isLoggedIn() ? this.auth.getToken() : ''),
      onNeedToken: () => this.ensureOnline(),
      onStatusChange: (connected, status) => {
        this.state.wsConnected = connected
        this.state.wsStatus = status || 'idle'
        if (connected) this.retryDelay = 10000
        this._emit()
      },
      onMessage: (msg) => {
        if (msg && msg.type === 'new_alarm' && this.onAlarmMessage) {
          try {
            this.onAlarmMessage(msg.data || {})
          } catch (e) {
            console.error('[Cloud] 报警推送处理异常: ' + e)
          }
        }
      },
      onError: (err) => {
        console.warn('[Cloud] 长连接异常: ' + ((err && err.data) || 'unknown'))
      }
    })
  }

  /**
   * 启动：登录并建立长连接（重复调用安全）
   */
  start() {
    this.init()
    this.ensureOnline()
  }

  /**
   * 确保已登录且长连接已建立；失败时按指数退避自动重试
   * @param {Function} callback (err, token)
   */
  ensureOnline(callback) {
    this.init()

    if (this.ensuring) {
      if (callback) this.ensureWaiters.push(callback)
      return
    }
    this.ensuring = true

    this.auth.ensureLogin((err, token) => {
      this.ensuring = false

      if (err) {
        const message = (err && err.message) || '登录失败'
        this.state.loggedIn = false
        this.state.lastError = message
        this._emit()
        this._scheduleRetry()
        this._flushEnsureWaiters(err)
        return
      }

      this.retryDelay = 10000
      this._clearRetry()

      // 登录成功（可能是复用本地未过期会话），同步一次状态
      this.state.loggedIn = true
      this.state.username = this.auth.getUsername()
      this.state.lastError = ''

      this._connectWS()
      this._emit()
      this._flushEnsureWaiters(null, token)
    })
  }

  /**
   * 通过 WebSocket 下发报警指令
   * @return {Boolean} 长连接不可用时返回 false（报警仍由 HTTP 上报落库）
   */
  reportAlarm(alarmPacket) {
    this.init()

    if (!this.ws.isConnected()) {
      // 连接异常时后台补建，不阻塞报警上报
      this.ensureOnline()
      return false
    }

    return this.ws.sendAlarm(alarmPacket)
  }

  isOnline() {
    return this.state.loggedIn && this.state.wsConnected
  }

  getStatus() {
    return {
      loggedIn: this.state.loggedIn,
      wsConnected: this.state.wsConnected,
      wsStatus: this.state.wsStatus,
      username: this.state.username,
      lastError: this.state.lastError,
      updatedAt: this.state.updatedAt
    }
  }

  getAuthHeader() {
    return this.auth.getAuthHeader()
  }

  onStatusChange(fn) {
    if (fn && this.listeners.indexOf(fn) < 0) this.listeners.push(fn)
  }

  offStatusChange(fn) {
    const index = this.listeners.indexOf(fn)
    if (index >= 0) this.listeners.splice(index, 1)
  }

  /**
   * 退出登录并断开长连接
   */
  logout() {
    this.auth.logout()
    this.ws.disconnect()
    this.state.loggedIn = false
    this.state.wsConnected = false
    this.state.wsStatus = 'disconnected'
    this._emit()
  }

  /**
   * 停止（页面销毁时调用，避免定时器泄漏）
   */
  stop() {
    this._clearRetry()
    this.ws.disconnect()
  }

  // ============================================
  // 内部方法
  // ============================================

  _connectWS() {
    const token = this.auth.getToken()
    if (!token) return
    if (this.ws.isConnected() || this.ws.isConnecting()) return
    this.ws.connect(token)
  }

  _scheduleRetry() {
    if (this.retryTimer) return
    const delay = this.retryDelay
    this.retryDelay = Math.min(this.retryDelay * 2, this.maxRetryDelay)
    console.log('[Cloud] ' + Math.round(delay / 1000) + 's 后重试登录')

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null
      this.ensureOnline()
    }, delay)
  }

  _clearRetry() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer)
      this.retryTimer = null
    }
  }

  _flushEnsureWaiters(err, token) {
    const waiters = this.ensureWaiters
    this.ensureWaiters = []
    waiters.forEach((fn) => {
      try {
        fn(err, token)
      } catch (e) {
        console.error('[Cloud] 登录回调异常: ' + e)
      }
    })
  }

  _emit() {
    this.state.updatedAt = Date.now()
    this._persist()

    const snapshot = this.getStatus()
    this.listeners.forEach((fn) => {
      try {
        fn(snapshot)
      } catch (e) {
        console.error('[Cloud] 状态回调异常: ' + e)
      }
    })
  }

  _persist() {
    storage.set({
      key: STORAGE_KEYS.CLOUD_STATUS,
      value: JSON.stringify(this.state),
      success: () => {},
      fail: () => {}
    })
  }
}

export default new CloudService()
