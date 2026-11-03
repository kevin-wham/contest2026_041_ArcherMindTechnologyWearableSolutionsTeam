/**
 * 设备绑定服务（单例）
 *
 * 手表出厂默认绑定 EH-WATCH-20260101，可在设置页修改为其它序列号；
 * 后续接入二维码扫码绑定时，扫码结果直接调用 bindByQRCode() 即可，
 * 上报链路（体征 / 报警 / 心跳 / 位置 / MQTT Topic）都会自动使用新的序列号。
 */
import storage from '@system.storage'
import { STORAGE_KEYS, DEVICE_BINDING } from '../utils/constants'

// 序列号格式：4~64 位字母、数字、-、_
const SN_PATTERN = /^[A-Za-z0-9_-]{4,64}$/

class DeviceBindingService {
  constructor() {
    this.deviceSn = DEVICE_BINDING.DEFAULT_SN
    this.elderlyId = DEVICE_BINDING.DEFAULT_ELDERLY_ID
    this.bound = false        // 是否被人工绑定（false = 使用出厂默认）
    this.source = 'default'   // default | manual | qrcode
    this.boundAt = 0

    this.loaded = false
    this.waiters = []
    this.listeners = []

    this._load()
  }

  // ============================================
  // 读取
  // ============================================

  getDeviceSN() {
    return this.deviceSn || DEVICE_BINDING.DEFAULT_SN
  }

  /**
   * 老人 ID：手工绑定时未知，返回空字符串，由服务端用 device_sn 反查
   */
  getElderlyId() {
    return this.bound ? (this.elderlyId || '') : DEVICE_BINDING.DEFAULT_ELDERLY_ID
  }

  isBound() {
    return this.bound
  }

  getStatus() {
    return {
      deviceSn: this.getDeviceSN(),
      elderlyId: this.getElderlyId(),
      bound: this.bound,
      source: this.source,
      boundAt: this.boundAt
    }
  }

  // ============================================
  // 绑定
  // ============================================

  /**
   * 绑定序列号
   * @param {String} deviceSn
   * @param {Object} options { elderlyId, source }
   * @return {Object} { ok, message }
   */
  bind(deviceSn, options = {}) {
    const sn = (deviceSn || '').trim().toUpperCase()

    if (!sn) {
      return { ok: false, message: '序列号不能为空' }
    }
    if (!SN_PATTERN.test(sn)) {
      return { ok: false, message: '序列号格式不正确（4~64 位字母/数字/-/_）' }
    }

    this.deviceSn = sn
    this.elderlyId = options.elderlyId || ''
    this.bound = true
    this.source = options.source || 'manual'
    this.boundAt = Date.now()

    this._persist()
    this._emit()
    console.log(`[Binding] 已绑定序列号: ${sn}（来源: ${this.source}）`)
    return { ok: true, message: '绑定成功' }
  }

  /**
   * 二维码扫码绑定（后续接入扫码后调用）
   * 支持三种内容：
   *   1) 纯序列号：EH-WATCH-20260101
   *   2) JSON：{"device_sn":"EH-WATCH-20260101","elderly_id":"xxx"}
   *   3) 绑定链接：https://xxx/bind?sn=EH-WATCH-20260101&elderly_id=xxx
   */
  bindByQRCode(qrContent, options = {}) {
    const text = (qrContent || '').trim()
    if (!text) return { ok: false, message: '二维码内容为空' }

    let sn = ''
    let elderlyId = ''

    // JSON
    if (text.indexOf('{') === 0) {
      try {
        const obj = JSON.parse(text)
        sn = obj.device_sn || obj.sn || obj.deviceSn || ''
        elderlyId = obj.elderly_id || obj.elderlyId || ''
      } catch (e) {
        return { ok: false, message: '二维码格式无法识别' }
      }
    } else if (text.indexOf('http') === 0 || text.indexOf('?') >= 0) {
      // 绑定链接
      const query = text.substring(text.indexOf('?') + 1)
      query.split('&').forEach((pair) => {
        const kv = pair.split('=')
        if (kv[0] === 'sn' || kv[0] === 'device_sn') sn = decodeURIComponent(kv[1] || '')
        if (kv[0] === 'elderly_id') elderlyId = decodeURIComponent(kv[1] || '')
      })
    } else {
      sn = text
    }

    if (!sn) return { ok: false, message: '二维码中未找到序列号' }

    return this.bind(sn, { elderlyId, source: 'qrcode' })
  }

  /**
   * 恢复出厂默认序列号
   */
  reset() {
    this.deviceSn = DEVICE_BINDING.DEFAULT_SN
    this.elderlyId = DEVICE_BINDING.DEFAULT_ELDERLY_ID
    this.bound = false
    this.source = 'default'
    this.boundAt = 0

    this._persist()
    this._emit()
    console.log(`[Binding] 已恢复默认序列号: ${this.deviceSn}`)
    return { ok: true, message: '已恢复默认' }
  }

  // ============================================
  // 订阅
  // ============================================

  onChange(fn) {
    if (fn && this.listeners.indexOf(fn) < 0) this.listeners.push(fn)
  }

  offChange(fn) {
    const index = this.listeners.indexOf(fn)
    if (index >= 0) this.listeners.splice(index, 1)
  }

  // ============================================
  // 内部方法
  // ============================================

  _load() {
    storage.get({
      key: STORAGE_KEYS.DEVICE_BINDING,
      success: (data) => {
        if (data) {
          try {
            const binding = JSON.parse(data)
            if (binding.device_sn) {
              this.deviceSn = binding.device_sn
              this.elderlyId = binding.elderly_id || ''
              this.bound = binding.bound === true
              this.source = binding.source || 'default'
              this.boundAt = binding.bound_at || 0
              console.log(`[Binding] 已加载绑定序列号: ${this.deviceSn}`)
            }
          } catch (e) {
            console.warn('[Binding] 绑定信息解析失败，使用默认值')
          }
        }
        this._onLoaded()
      },
      fail: () => this._onLoaded()
    })
  }

  _onLoaded() {
    this.loaded = true
    const waiters = this.waiters
    this.waiters = []
    waiters.forEach((fn) => fn(this.getStatus()))
  }

  _persist() {
    storage.set({
      key: STORAGE_KEYS.DEVICE_BINDING,
      value: JSON.stringify({
        device_sn: this.deviceSn,
        elderly_id: this.elderlyId,
        bound: this.bound,
        source: this.source,
        bound_at: this.boundAt
      }),
      success: () => {},
      fail: (data, code) => console.error(`[Binding] 保存绑定信息失败: code=${code}`)
    })
  }

  _emit() {
    const snapshot = this.getStatus()
    this.listeners.forEach((fn) => {
      try {
        fn(snapshot)
      } catch (e) {
        console.error('[Binding] 回调异常: ' + e)
      }
    })
  }
}

export default new DeviceBindingService()
