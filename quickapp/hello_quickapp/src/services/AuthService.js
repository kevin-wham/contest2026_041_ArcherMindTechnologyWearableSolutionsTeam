/**
 * 认证服务
 *
 * 职责：
 *   1. 使用账号密码登录后台服务器（POST /v1/admin/login）
 *   2. 缓存 access_token / refresh_token 到本地存储
 *   3. Token 到期前自动刷新，刷新失败自动重新登录
 *   4. 为其它服务提供 Bearer 鉴权头
 *
 * 说明：WebSocket 连接需要用 access_token 鉴权，所以必须先登录成功。
 */
import fetch from '@system.fetch'
import storage from '@system.storage'
import { API_CONFIG, STORAGE_KEYS } from '../utils/constants'

export class AuthService {
  constructor() {
    this.accessToken = ''
    this.refreshTokenValue = ''
    this.expiresAt = 0
    this.username = API_CONFIG.AUTH.USERNAME
    this.password = API_CONFIG.AUTH.PASSWORD
    this.user = null

    this.loggingIn = false
    this.loginWaiters = []
    this.refreshTimer = null
    this.onStatusChange = null

    this.loaded = false
    this.loadWaiters = []
  }

  /**
   * 初始化：加载本地会话与账号
   * @param {Object} options { onStatusChange(loggedIn, message) }
   */
  init(options = {}) {
    this.onStatusChange = options.onStatusChange || null
    this._loadPersisted()
  }

  // ============================================
  // 状态查询
  // ============================================

  getToken() {
    return this.accessToken || ''
  }

  getUsername() {
    return this.username || API_CONFIG.AUTH.USERNAME
  }

  isLoggedIn() {
    // 预留 30 秒余量，避免临界过期
    return !!this.accessToken && this.expiresAt > Date.now() + 30000
  }

  isExpired() {
    return !!this.expiresAt && this.expiresAt <= Date.now()
  }

  getAuthHeader() {
    return this.accessToken ? { Authorization: 'Bearer ' + this.accessToken } : {}
  }

  getStatus() {
    return {
      loggedIn: this.isLoggedIn(),
      username: this.username,
      expiresAt: this.expiresAt,
      user: this.user
    }
  }

  /**
   * 修改登录账号（持久化，下次启动生效）
   */
  setCredentials(username, password) {
    if (username) this.username = username
    if (password) this.password = password
    storage.set({
      key: STORAGE_KEYS.AUTH_CREDENTIALS,
      value: JSON.stringify({ username: this.username, password: this.password }),
      success: () => {},
      fail: () => {}
    })
  }

  // ============================================
  // 登录 / 刷新 / 登出
  // ============================================

  /**
   * 确保已登录：优先复用本地 Token，其次刷新，最后重新登录
   * @param {Function} callback (err, token)
   */
  ensureLogin(callback) {
    this._whenLoaded(() => {
      if (this.isLoggedIn()) {
        if (callback) callback(null, this.accessToken)
        return
      }

      if (this.refreshTokenValue) {
        this.refresh((err) => {
          if (!err) {
            if (callback) callback(null, this.accessToken)
            return
          }
          this.login(this.username, this.password, callback)
        })
        return
      }

      this.login(this.username, this.password, callback)
    })
  }

  /**
   * 账号密码登录
   * @param {String} username
   * @param {String} password
   * @param {Function} callback (err, token)
   */
  login(username, password, callback) {
    if (username) this.username = username
    if (password) this.password = password

    // 同一时刻只允许一个登录请求，其余排队
    if (this.loggingIn) {
      if (callback) this.loginWaiters.push(callback)
      return
    }

    this.loggingIn = true
    const url = API_CONFIG.SERVICES.AUTH + API_CONFIG.AUTH.LOGIN
    const body = { username: this.username, password: this.password }

    console.log('[Auth] 正在登录服务器: ' + this.username)

    this._request(url, body, null, (err, res) => {
      this.loggingIn = false

      if (err) {
        const message = (err && err.message) || '网络异常，登录失败'
        console.error('[Auth] 登录失败: ' + message)
        this._emit(false, message)
        this._flushLoginWaiters({ message: message })
        return
      }

      const data = res && res.data
      if (!data || !data.access_token) {
        const message = (res && res.message) || '登录失败'
        console.error('[Auth] 登录失败: ' + message)
        this._emit(false, message)
        this._flushLoginWaiters({ message: message })
        return
      }

      this._applySession(data)
      console.log('[Auth] 登录成功，Token 有效期 ' + Math.round((this.expiresAt - Date.now()) / 1000) + 's')
      this._emit(true, '已登录')
      this._flushLoginWaiters(null, this.accessToken)
    })
  }

  /**
   * 使用 refresh_token 刷新 access_token
   * @param {Function} callback (err, token)
   */
  refresh(callback) {
    if (!this.refreshTokenValue) {
      if (callback) callback({ message: '无刷新凭证' })
      return
    }

    const url = API_CONFIG.SERVICES.AUTH + API_CONFIG.AUTH.REFRESH
    const body = { refresh_token: this.refreshTokenValue }

    this._request(url, body, null, (err, res) => {
      const data = res && res.data
      if (err || !data || !data.access_token) {
        console.warn('[Auth] Token 刷新失败: ' + ((err && err.message) || '响应异常'))
        if (callback) callback(err || { message: '刷新失败' })
        return
      }

      this._applySession(data)
      console.log('[Auth] Token 已刷新')
      this._emit(true, '已登录')
      if (callback) callback(null, this.accessToken)
    })
  }

  /**
   * 退出登录（清除本地会话）
   */
  logout() {
    const header = this.getAuthHeader()
    if (this.accessToken) {
      this._request(
        API_CONFIG.SERVICES.AUTH + API_CONFIG.AUTH.LOGOUT,
        {},
        header,
        () => {}
      )
    }
    this.clearSession()
    this._emit(false, '已退出登录')
  }

  /**
   * 清除本地会话
   */
  clearSession() {
    this.accessToken = ''
    this.refreshTokenValue = ''
    this.expiresAt = 0
    this.user = null
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer)
      this.refreshTimer = null
    }
    storage.delete({
      key: STORAGE_KEYS.AUTH_SESSION,
      success: () => {},
      fail: () => {}
    })
  }

  // ============================================
  // 内部方法
  // ============================================

  _applySession(data) {
    const ttl = data.expires_in ? Number(data.expires_in) : 900
    this.accessToken = data.access_token
    this.refreshTokenValue = data.refresh_token || this.refreshTokenValue
    this.expiresAt = Date.now() + ttl * 1000
    this.user = data.user || this.user
    if (data.user && data.user.username) this.username = data.user.username

    this._persist()
    this._scheduleRefresh()
  }

  _persist() {
    const session = {
      access_token: this.accessToken,
      refresh_token: this.refreshTokenValue,
      expires_at: this.expiresAt,
      username: this.username,
      user: this.user
    }
    storage.set({
      key: STORAGE_KEYS.AUTH_SESSION,
      value: JSON.stringify(session),
      success: () => {},
      fail: (data, code) => console.error('[Auth] 保存会话失败: code=' + code)
    })
  }

  _loadPersisted() {
    storage.get({
      key: STORAGE_KEYS.AUTH_SESSION,
      success: (data) => {
        if (data) {
          try {
            const session = JSON.parse(data)
            this.accessToken = session.access_token || ''
            this.refreshTokenValue = session.refresh_token || ''
            this.expiresAt = session.expires_at || 0
            this.username = session.username || this.username
            this.user = session.user || null
            console.log('[Auth] 已加载本地会话，登录' + (this.isLoggedIn() ? '有效' : '已过期'))
          } catch (e) {
            console.warn('[Auth] 本地会话解析失败')
          }
        }
        this._loadCredentials()
      },
      fail: () => this._loadCredentials()
    })
  }

  _loadCredentials() {
    storage.get({
      key: STORAGE_KEYS.AUTH_CREDENTIALS,
      success: (data) => {
        if (data) {
          try {
            const cred = JSON.parse(data)
            if (cred.username) this.username = cred.username
            if (cred.password) this.password = cred.password
          } catch (e) {}
        }
        this._onLoaded()
      },
      fail: () => this._onLoaded()
    })
  }

  _onLoaded() {
    this.loaded = true
    if (this.isLoggedIn()) this._scheduleRefresh()
    const waiters = this.loadWaiters
    this.loadWaiters = []
    waiters.forEach((fn) => fn())
  }

  _whenLoaded(callback) {
    if (this.loaded) {
      callback()
    } else {
      this.loadWaiters.push(callback)
    }
  }

  _scheduleRefresh(delay) {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer)
      this.refreshTimer = null
    }
    if (!this.accessToken) return

    let wait = delay
    if (wait === undefined) {
      wait = Math.max(this.expiresAt - Date.now() - 60000, 30000)
    }

    this.refreshTimer = setTimeout(() => {
      this.refreshTimer = null
      this.refresh(() => {})
    }, wait)
  }

  _flushLoginWaiters(err, token) {
    const waiters = this.loginWaiters
    this.loginWaiters = []
    waiters.forEach((fn) => {
      try {
        fn(err, token)
      } catch (e) {
        console.error('[Auth] 登录回调异常: ' + e)
      }
    })
  }

  _emit(loggedIn, message) {
    if (this.onStatusChange) {
      try {
        this.onStatusChange(loggedIn, message)
      } catch (e) {
        console.error('[Auth] 状态回调异常: ' + e)
      }
    }
  }

  /**
   * 发送 JSON 请求
   * @param {String} url
   * @param {Object} payload
   * @param {Object} header
   * @param {Function} callback (err, body)
   */
  _request(url, payload, header, callback) {
    const requestHeader = Object.assign({ 'Content-Type': 'application/json' }, header || {})

    fetch.fetch({
      url: url,
      method: 'POST',
      header: requestHeader,
      data: JSON.stringify(payload || {}),
      responseType: 'json',
      success: (response) => {
        let body = response && response.data !== undefined ? response.data : response
        if (typeof body === 'string') {
          try {
            body = JSON.parse(body)
          } catch (e) {
            body = null
          }
        }

        const httpCode = response && response.code
        const bizCode = body && body.code
        const ok = (bizCode !== undefined && bizCode !== null)
          ? (bizCode === 0 || bizCode === 200)
          : (httpCode === 200 || httpCode === 201)

        if (ok) {
          callback(null, body)
        } else {
          callback({
            code: bizCode !== undefined ? bizCode : httpCode,
            message: (body && body.message) || ('HTTP ' + httpCode)
          })
        }
      },
      fail: (data, code) => {
        callback({ code: code, message: '网络请求失败 (code=' + code + ')' })
      }
    })
  }
}

export default AuthService
