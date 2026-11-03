/**
 * 数据上传服务
 * 匹配服务器端 API 接口文档 v2.0
 *
 * 支持两种上传模式（通过调试开关切换）：
 *   - HTTP 模式（模拟器调试）：通过 @system.fetch POST 到服务器
 *   - MQTT 模式（真机）：通过 MQTT 发布到 Broker
 *
 * 服务器地址：http://101.35.231.154/api/v1
 * MQTT Broker：ws://101.35.231.154:8083/mqtt
 */
import fetch from '@system.fetch'
import network from '@system.network'
import battery from '@system.battery'
import storage from '@system.storage'
import { API_CONFIG, STORAGE_KEYS, UPLOAD_MODE } from '../utils/constants'
import cloudService from './CloudService'
import deviceBinding from './DeviceBindingService'

export class UploadService {
  constructor() {
    // 乐观默认：network.getType 可能失败/未声明权限，
    // 若默认 false 会导致报警被静默缓存而永远发不出去
    this.isNetworkAvailable = true
    this.pendingAlarms = []
    this.pendingVitals = []
    this.retryCount = {}
    this.maxRetry = 3
    this.retryDelays = [1000, 2000, 4000]
    this.uploadCallback = null
    this.uploadLog = []
    this.seq = 0 // 数据序号

    // === 新增：上传模式与 MQTT ===
    this.uploadMode = UPLOAD_MODE.HTTP  // 默认 HTTP 模式
    this.mqttService = null              // MQTT 服务实例

    // === 云端连接（登录 + WebSocket 实时通道）===
    this.cloudService = cloudService
    this.authService = null
  }

  /**
   * 绑定认证服务（用于给 HTTP 请求附加 Bearer Token）
   */
  setAuthService(authService) {
    this.authService = authService
  }

  /**
   * 统一请求头：需要鉴权时附加 Authorization
   */
  _jsonHeader() {
    const header = { 'Content-Type': 'application/json' }
    if (this.authService) {
      const auth = this.authService.getAuthHeader()
      if (auth.Authorization) header.Authorization = auth.Authorization
    } else if (this.cloudService) {
      const auth = this.cloudService.getAuthHeader()
      if (auth.Authorization) header.Authorization = auth.Authorization
    }
    return header
  }

  // ============================================
  // 接口地址（微服务直连）
  // ============================================
  _vitalUrl() {
    return API_CONFIG.SERVICES.VITAL + API_CONFIG.ENDPOINTS.V1.VITAL_SIGN
  }

  _alarmUrl() {
    return API_CONFIG.SERVICES.ALARM + API_CONFIG.ENDPOINTS.V1.ALARM_REPORT
  }

  _heartbeatUrl() {
    return API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_HEARTBEAT
  }

  _onlineUrl() {
    return API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_ONLINE
  }

  _positionUrl() {
    return API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_POSITION
  }

  /**
   * 初始化上传服务
   */
  init(callback) {
    this.uploadCallback = callback

    // === 新增：加载上次的保存的上传模式 ===
    this._loadUploadMode()

    // 检查网络状态
    network.getType({
      success: (data) => {
        this.isNetworkAvailable = data.available !== false
        this._notifyNetworkChange(this.isNetworkAvailable)
        console.log(`[Upload] 网络状态: ${this.isNetworkAvailable ? '已连接' : '未连接'}`)
      },
      fail: (data, code) => {
        console.error(`[Upload] 获取网络状态失败: code=${code}`)
        // 保持乐观默认，宁可请求失败后持久化重试，也不静默丢数据
      }
    })

    // 监听网络变化
    network.subscribe({
      callback: (data) => {
        this.isNetworkAvailable = data.available
        this._notifyNetworkChange(this.isNetworkAvailable)
        console.log(`[Upload] 网络变化: ${data.available ? '已连接' : '已断开'}`)
        if (data.available) {
          this.retryPendingData()
        }
      },
      fail: (data, code) => {
        console.error(`[Upload] 网络监听失败: code=${code}`)
      }
    })

    // 启动时补传一次持久化的报警（上次离线/失败遗留）
    this._flushPendingAlarms()
  }

  /**
   * 通知外部网络状态变化
   */
  _notifyNetworkChange(available) {
    if (this.uploadCallback && typeof this.uploadCallback.onNetworkChange === 'function') {
      this.uploadCallback.onNetworkChange(available)
    }
  }

  // ============================================
  // 上传模式管理
  // ============================================

  /**
   * 设置上传模式
   * @param {string} mode - 'http' 或 'mqtt'
   */
  setUploadMode(mode) {
    if (mode !== UPLOAD_MODE.HTTP && mode !== UPLOAD_MODE.MQTT) {
      console.warn(`[Upload] 无效的上传模式: ${mode}，保持当前模式`)
      return
    }

    const oldMode = this.uploadMode
    this.uploadMode = mode

    // 持久化
    storage.set({
      key: STORAGE_KEYS.UPLOAD_MODE,
      value: mode,
      success: () => {},
      fail: () => {}
    })

    console.log(`[Upload] 上传模式切换: ${oldMode} → ${mode}`)

    // 切换到 MQTT 时尝试连接
    if (mode === UPLOAD_MODE.MQTT && this.mqttService && !this.mqttService.isConnected()) {
      this.mqttService.connect()
    }
  }

  /**
   * 获取当前上传模式
   */
  getUploadMode() {
    return this.uploadMode
  }

  /**
   * 绑定 MQTT 服务实例
   * @param {MqttService} mqttService
   */
  setMqttService(mqttService) {
    this.mqttService = mqttService
    console.log('[Upload] MQTT 服务已绑定')
  }

  /**
   * 从 storage 加载上传模式
   * 模拟器环境强制使用 HTTP 模式（MQTT 无 WebSocket 支持）
   */
  _loadUploadMode() {
    storage.get({
      key: STORAGE_KEYS.UPLOAD_MODE,
      success: (data) => {
        if (data === UPLOAD_MODE.HTTP || data === UPLOAD_MODE.MQTT) {
          this.uploadMode = data
          console.log(`[Upload] 加载上传模式: ${data}`)
        } else {
          // storage 无值或异常值 → 强制 HTTP
          this.uploadMode = UPLOAD_MODE.HTTP
          this._persistUploadMode()
        }
      },
      fail: () => {
        // storage 不存在 → 强制 HTTP 并持久化
        this.uploadMode = UPLOAD_MODE.HTTP
        this._persistUploadMode()
      }
    })
  }

  /**
   * 持久化上传模式到 storage
   */
  _persistUploadMode() {
    storage.set({
      key: STORAGE_KEYS.UPLOAD_MODE,
      value: this.uploadMode,
      success: () => {},
      fail: () => {}
    })
  }

  // ============================================
  // 体征数据上报（POST /api/v1/vitals/sign）
  // ============================================

  /**
   * 上传体征数据
   * 匹配服务器接口：POST /api/v1/vitals/sign 或 MQTT device/{sn}/vital
   * @param {Object} vitalData - 单条体征数据
   * @param {Function} successCallback
   * @param {Function} failCallback
   */
  uploadVitalSigns(vitalData, successCallback, failCallback) {
    if (!this.isNetworkAvailable) {
      console.log('[Upload] 网络不可用，体征数据已缓存')
      this.cachePendingVital(vitalData)
      if (failCallback) failCallback('network_unavailable')
      return
    }

    this.getBatteryInfo((batteryLevel) => {
      const packet = this.buildVitalPacket(vitalData, batteryLevel)

      if (this.uploadMode === UPLOAD_MODE.MQTT) {
        // === MQTT 模式 ===
        this._uploadViaMQTT(
          API_CONFIG.MQTT_TOPICS.VITAL(deviceBinding.getDeviceSN()),
          packet,
          'vital',
          successCallback,
          failCallback
        )
      } else {
        // === HTTP 模式 ===
        this._uploadViaHTTP(
          this._vitalUrl(),
          packet,
          'vital',
          successCallback,
          failCallback
        )
      }
    })
  }

  /**
   * HTTP 上传（内部方法）
   */
  _uploadViaHTTP(url, packet, type, successCallback, failCallback) {
    this.logUpload(type, packet)
    const modeTag = '[HTTP]'

    fetch.fetch({
      url: url,
      method: 'POST',
      header: this._jsonHeader(),
      data: JSON.stringify(packet),
      responseType: 'json',
      success: (response) => {
        const res = response.data || response
        if (res.code === 0 || res.code === 200 || response.code === 200) {
          console.log(`${modeTag} ${type}上报成功`)
          if (successCallback) successCallback(res)
        } else {
          console.error(`${modeTag} ${type}上报失败: code=${res.code} msg=${res.message}`)
          this.handleRetry(url, packet, failCallback)
        }
      },
      fail: (data, code) => {
        console.error(`${modeTag} ${type}上报请求失败: code=${code}`)
        this.handleRetry(url, packet, failCallback)
      }
    })
  }

  /**
   * MQTT 上传（内部方法）
   */
  _uploadViaMQTT(topic, packet, type, successCallback, failCallback) {
    const modeTag = '[MQTT]'
    const fallback = () => {
      this._uploadViaHTTP(
        type === 'vital' ? this._vitalUrl() : this._alarmUrl(),
        packet, type, successCallback, failCallback
      )
    }

    if (!this.mqttService) {
      console.error(`${modeTag} MQTT 服务未绑定，回退到 HTTP`)
      fallback()
      return
    }

    this.logUpload(`mqtt_${type}`, { topic, packet })

    const ok = this.mqttService.publish(topic, packet, {
      cache: false, // 未连接时直接由 fallback 走 HTTP，避免缓存重复发送
      onSuccess: () => {
        console.log(`${modeTag} ${type}上报成功 → ${topic}`)
        if (successCallback) successCallback({ code: 0, mode: 'mqtt' })
      },
      onError: (error) => {
        console.warn(`${modeTag} ${type}发布失败(${error})，回退 HTTP`)
        fallback()
      }
    })

    if (!ok) {
      // MQTT 未连接：直接走 HTTP，保证数据即时到达
      console.warn(`${modeTag} ${type}MQTT 未连接，回退 HTTP`)
      fallback()
    }
  }

  /**
   * 构建体征数据包
   * 匹配服务器字段：msg_id, device_sn, timestamp, seq, heart_rate, spo2, temperature, steps, battery
   */
  buildVitalPacket(data, batteryLevel) {
    this.seq++
    const now = new Date()

    return {
      device_sn: deviceBinding.getDeviceSN(),
      elderly_id: deviceBinding.getElderlyId(),
      timestamp: Date.now(),           // 毫秒时间戳，服务端 FROM_UNIXTIME(?/1000) 使用
      heart_rate: Math.round(data.heartRate || 0),
      spo2: Math.round(data.spo2 || 0),
      temperature: parseFloat((data.temperature || 0).toFixed(1)),
      steps: data.steps || 0,
      activity_level: this.calcActivityLevel(data.steps),
      posture: 'standing',
      battery: batteryLevel >= 0 ? batteryLevel : 100,
      signal_strength: 72
    }
  }

  /**
   * 根据步数估算活动等级
   */
  calcActivityLevel(steps) {
    if (steps > 8000) return 5
    if (steps > 5000) return 4
    if (steps > 3000) return 3
    if (steps > 1000) return 2
    return 1
  }

  // ============================================
  // 报警数据上报（POST /api/v1/alarms/report）
  // ============================================

  /**
   * 上传报警数据
   * 匹配服务器接口：POST /api/v1/alarms/report 或 MQTT device/{sn}/alarm
   * @param {Object} alarmPacket - 报警数据
   * @param {Function} successCallback - 服务器确认收到后回调
   * @param {Function} failCallback - 发送失败（已持久化，等待补传）后回调
   */
  uploadAlarm(alarmPacket, successCallback, failCallback) {
    let finished = false
    const done = (err) => {
      if (finished) return
      finished = true
      if (err) {
        // 失败：持久化，下次启动/网络恢复后自动补传，报警不丢
        this._storePendingAlarm(alarmPacket)
        if (failCallback) failCallback(err)
      } else if (successCallback) {
        successCallback()
      }
    }

    // 实时通道：WebSocket 在线时立即下发报警指令（秒级送达）
    this._sendAlarmViaCloud(alarmPacket)

    if (!this.isNetworkAvailable) {
      console.log('[Upload] 网络不可用，报警已持久化待补传')
      done('network_unavailable')
      return
    }

    // MQTT 模式且已连接：走 Topic device/{sn}/alarm，失败自动回退 HTTP
    if (this.uploadMode === UPLOAD_MODE.MQTT && this.mqttService && this.mqttService.isConnected()) {
      this._uploadViaMQTT(
        API_CONFIG.MQTT_TOPICS.ALARM(deviceBinding.getDeviceSN()),
        alarmPacket,
        'alarm',
        () => {
          console.log(`[Upload] 报警上报成功(MQTT): type=${alarmPacket.alarm_type} level=${alarmPacket.severity}`)
          done(null)
        },
        (error) => {
          console.error(`[Upload] 报警上报失败(MQTT): ${error}`)
          done(error || 'mqtt_failed')
        }
      )
      return
    }

    // HTTP 模式（写入 alarm_records，保证报警不丢）
    this._uploadAlarmHTTP(alarmPacket, done)
  }

  /**
   * HTTP 上报报警（内部方法）
   */
  _uploadAlarmHTTP(alarmPacket, done) {
    const url = this._alarmUrl()
    this.logUpload('alarm', alarmPacket)

    fetch.fetch({
      url: url,
      method: 'POST',
      header: this._jsonHeader(),
      data: JSON.stringify(alarmPacket),
      responseType: 'json',
      success: (response) => {
        const res = response.data || response
        const code = res.code !== undefined ? res.code : response.code
        if (code === 0 || code === 200) {
          console.log(`[Upload] 报警上报成功(HTTP): type=${alarmPacket.alarm_type} level=${alarmPacket.severity}`)
          done(null)
        } else {
          console.error(`[Upload] 报警上报失败: code=${code}`)
          done(`code=${code}`)
        }
      },
      fail: (data, code) => {
        console.error(`[Upload] 报警上报请求失败: code=${code}`)
        done(`request_failed=${code}`)
      }
    })
  }

  /**
   * 报警持久化（写入 storage，页面销毁/重启后仍可补传）
   */
  _storePendingAlarm(alarmPacket) {
    storage.get({
      key: STORAGE_KEYS.ALARM_PENDING,
      success: (data) => {
        let list = []
        if (data) {
          try { list = JSON.parse(data) } catch (e) { list = [] }
        }
        if (!Array.isArray(list)) list = []
        list.push(alarmPacket)
        if (list.length > 50) list = list.slice(-50)
        storage.set({
          key: STORAGE_KEYS.ALARM_PENDING,
          value: JSON.stringify(list),
          success: () => {},
          fail: () => {}
        })
      },
      fail: () => {
        storage.set({
          key: STORAGE_KEYS.ALARM_PENDING,
          value: JSON.stringify([alarmPacket]),
          success: () => {},
          fail: () => {}
        })
      }
    })
  }

  /**
   * 补传持久化的报警
   */
  _flushPendingAlarms() {
    storage.get({
      key: STORAGE_KEYS.ALARM_PENDING,
      success: (data) => {
        let list = []
        if (data) {
          try { list = JSON.parse(data) } catch (e) { list = [] }
        }
        if (!Array.isArray(list) || list.length === 0) return

        console.log(`[Upload] 补传 ${list.length} 条持久化报警`)
        // 先清空，失败的会重新入库，避免重复堆积
        storage.delete({ key: STORAGE_KEYS.ALARM_PENDING, success: () => {}, fail: () => {} })

        list.forEach((alarm) => {
          this._sendAlarmViaCloud(alarm)
          this._uploadAlarmHTTP(alarm, (err) => {
            if (err) this._storePendingAlarm(alarm)
          })
        })
      },
      fail: () => {}
    })
  }

  /**
   * 通过 WebSocket 长连接下发报警指令
   */
  _sendAlarmViaCloud(alarmPacket) {
    if (!this.cloudService) return false
    try {
      return this.cloudService.reportAlarm(alarmPacket)
    } catch (e) {
      console.warn(`[Upload] WebSocket 下发报警失败: ${e}`)
      return false
    }
  }

  /**
   * 构建报警数据包
   * 匹配服务器字段：device_sn, elderly_id, timestamp, alarm_type, severity, heart_rate, spo2, temperature, latitude, longitude, battery
   */
  buildAlarmPacket(type, level, vitals, location, extra) {
    const typeMap = {
      'sos': 'SOS',
      'fall': 'FALL',
      'abnormal': 'VITAL_ABNORMAL'
    }

    // 无效体征（-1 / 空）统一上报 0，避免服务端出现负数
    const hr = vitals && vitals.heartRate > 0 ? Math.round(vitals.heartRate) : 0
    const ox = vitals && vitals.spo2 > 0 ? Math.round(vitals.spo2) : 0
    const temp = vitals && vitals.temperature > 0 ? parseFloat(vitals.temperature.toFixed(1)) : 0

    const packet = {
      device_sn: deviceBinding.getDeviceSN(),
      elderly_id: deviceBinding.getElderlyId(),
      timestamp: Date.now(),
      alarm_type: typeMap[type] || type.toUpperCase(),
      severity: level,
      heart_rate: hr,
      spo2: ox,
      temperature: temp,
      battery: 85
    }

    // 位置信息（可选）
    if (location) {
      packet.latitude = location.latitude
      packet.longitude = location.longitude
    }

    // 额外信息
    if (extra) {
      if (extra.trigger) packet.trigger = extra.trigger
      if (extra.fall_probability !== undefined) packet.fall_probability = extra.fall_probability
    }

    return packet
  }

  // ============================================
  // 位置上报（MQTT: device/{sn}/position, HTTP: POST /api/v1/devices/position）
  // ============================================

  /**
   * 上传位置数据
   * 匹配服务器字段：device_sn, timestamp, latitude, longitude, accuracy, position_type
   */
  uploadPosition(location) {
    if (!this.isNetworkAvailable || !location) return

    const packet = {
      device_sn: deviceBinding.getDeviceSN(),
      timestamp: Date.now(),
      latitude: location.latitude,
      longitude: location.longitude,
      altitude: location.altitude || 0,
      accuracy: location.accuracy || 10,
      position_type: 'GPS'
    }

    if (this.uploadMode === UPLOAD_MODE.MQTT && this.mqttService) {
      this.mqttService.publish(
        API_CONFIG.MQTT_TOPICS.POSITION(deviceBinding.getDeviceSN()),
        packet
      )
      console.log(`[MQTT] 位置上报: ${location.latitude}, ${location.longitude}`)
    } else {
      // HTTP 模式：POST 到服务器
      const url = this._positionUrl()
      fetch.fetch({
        url: url,
        method: 'POST',
        header: this._jsonHeader(),
        data: JSON.stringify(packet),
        responseType: 'json',
        success: () => {},
        fail: () => {}
      })
      console.log(`[HTTP] 位置上报: ${location.latitude}, ${location.longitude}`)
    }
  }

  // ============================================
  // 设备心跳上报（MQTT: device/{sn}/heartbeat, HTTP: POST /api/v1/devices/heartbeat）
  // ============================================

  /**
   * 上传心跳数据
   */
  uploadHeartbeat(batteryLevel) {
    if (!this.isNetworkAvailable) return

    const packet = {
      device_sn: deviceBinding.getDeviceSN(),
      timestamp: Date.now(),
      battery: batteryLevel >= 0 ? batteryLevel : 100,
      signal_strength: 72,
      firmware_version: '1.0.0'
    }

    if (this.uploadMode === UPLOAD_MODE.MQTT && this.mqttService) {
      this.mqttService.publish(
        API_CONFIG.MQTT_TOPICS.HEARTBEAT(deviceBinding.getDeviceSN()),
        packet
      )
      console.log(`[MQTT] 心跳上报: battery=${packet.battery}%`)
    } else {
      const url = this._heartbeatUrl()
      fetch.fetch({
        url: url,
        method: 'POST',
        header: this._jsonHeader(),
        data: JSON.stringify(packet),
        responseType: 'json',
        success: () => {},
        fail: () => {}
      })
    }
  }

  // ============================================
  // 离线数据缓存与补传
  // ============================================

  cachePendingVital(data) {
    storage.get({
      key: STORAGE_KEYS.VITAL_CACHE,
      success: (cached) => {
        let list = []
        if (cached) {
          try { list = JSON.parse(cached) } catch (e) { list = [] }
        }
        list.push(data)
        if (list.length > 500) list = list.slice(-500)
        storage.set({
          key: STORAGE_KEYS.VITAL_CACHE,
          value: JSON.stringify(list),
          success: () => {},
          fail: () => {}
        })
      },
      fail: () => {
        storage.set({
          key: STORAGE_KEYS.VITAL_CACHE,
          value: JSON.stringify([data]),
          success: () => {},
          fail: () => {}
        })
      }
    })
  }

  /**
   * 网络恢复后重传缓存数据
   */
  retryPendingData() {
    // 重传体征数据
    storage.get({
      key: STORAGE_KEYS.VITAL_CACHE,
      success: (data) => {
        if (data) {
          try {
            const list = JSON.parse(data)
            if (list.length > 0) {
              console.log(`[Upload] 补传 ${list.length} 条缓存体征数据`)
              list.forEach(item => {
                this.uploadVitalSigns(item)
              })
              storage.delete({ key: STORAGE_KEYS.VITAL_CACHE })
            }
          } catch (e) {}
        }
      },
      fail: () => {}
    })

    // 重传报警数据（内存 + 持久化）
    if (this.pendingAlarms.length > 0) {
      const alarms = [...this.pendingAlarms]
      this.pendingAlarms = []
      alarms.forEach(alarm => this._storePendingAlarm(alarm))
    }
    this._flushPendingAlarms()
  }

  // ============================================
  // 工具方法
  // ============================================

  getBatteryInfo(callback) {
    battery.getInfo({
      success: (data) => callback(data.level || 100),
      fail: () => callback(-1)
    })
  }

  handleRetry(url, data, failCallback) {
    const dataKey = `${url}_${JSON.stringify(data).substring(0, 30)}`
    const currentRetry = this.retryCount[dataKey] || 0

    if (currentRetry < this.maxRetry) {
      this.retryCount[dataKey] = currentRetry + 1
      const delay = this.retryDelays[currentRetry]
      console.log(`[Upload] 重试 (${currentRetry + 1}/${this.maxRetry})，延迟 ${delay}ms`)
      setTimeout(() => {
        fetch.fetch({
          url: url,
          method: 'POST',
          header: { 'Content-Type': 'application/json' },
          data: JSON.stringify(data),
          responseType: 'json',
          success: (response) => {
            const res = response.data || response
            if (res.code === 0 || res.code === 200 || response.code === 200) {
              delete this.retryCount[dataKey]
              console.log(`[Upload] 重试上传成功`)
            } else {
              this.handleRetry(url, data, failCallback)
            }
          },
          fail: () => {
            this.handleRetry(url, data, failCallback)
          }
        })
      }, delay)
    } else {
      delete this.retryCount[dataKey]
      console.error(`[Upload] 重试次数耗尽: ${url}`)
      if (failCallback) failCallback('max_retry_exceeded')
    }
  }

  /**
   * 记录上传日志（便于调试查看）
   */
  logUpload(type, data) {
    const logEntry = {
      type: type,
      timestamp: Date.now(),
      timeStr: new Date().toLocaleTimeString(),
      data: data
    }

    this.uploadLog.unshift(logEntry)
    if (this.uploadLog.length > 50) {
      this.uploadLog = this.uploadLog.slice(0, 50)
    }

    storage.set({
      key: STORAGE_KEYS.UPLOAD_LOG,
      value: JSON.stringify(this.uploadLog),
      success: () => {},
      fail: () => {}
    })
  }

  getUploadLog() {
    return this.uploadLog
  }

  /**
   * 停止上传服务
   */
  stop() {
    network.unsubscribe({
      success: () => {},
      fail: (data, code) => {
        console.error(`[Upload] 取消网络监听失败: code=${code}`)
      }
    })
  }
}

export default UploadService
