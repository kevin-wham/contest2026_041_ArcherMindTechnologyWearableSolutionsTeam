/**
 * 每日签到上报服务
 *
 * 服务端签到记录写入 signin_records，目前有两条通道：
 *   1) MQTT：topic device/{sn}/signin（真机 MQTT 模式，首页负责补传）
 *   2) HTTP：POST /v1/signin/proxy（本服务实现，带管理员 Token 代签到）
 *
 * 本服务负责 HTTP 通道：解析老人 ID → 调用代签到接口 → 回调结果。
 */
import fetch from '@system.fetch'
import { API_CONFIG } from '../utils/constants'
import cloudService from './CloudService'
import deviceBinding from './DeviceBindingService'

class SigninService {
  /**
   * 上报一次签到
   * @param {Object} record { date, time, timestamp, method }
   * @param {Function} callback (err, data)
   */
  report(record, callback) {
    // 确保认证服务已初始化（幂等）
    cloudService.init()

    // 复用 CloudService 内的认证实例（AuthService 默认导出的是类，不是单例）
    const auth = cloudService.auth

    auth.ensureLogin((err) => {
      if (err) {
        if (callback) callback(err)
        return
      }

      this._resolveElderlyId((elderlyId) => {
        if (!elderlyId) {
          if (callback) callback(new Error('未找到设备绑定的老人'))
          return
        }

        const header = { 'Content-Type': 'application/json' }
        const authHeader = auth.getAuthHeader()
        if (authHeader.Authorization) header.Authorization = authHeader.Authorization

        fetch.fetch({
          url: API_CONFIG.SERVICES.SIGNIN + API_CONFIG.ENDPOINTS.V1.SIGNIN_PROXY,
          method: 'POST',
          header: header,
          data: JSON.stringify({
            elderly_id: elderlyId,
            note: 'device:' + deviceBinding.getDeviceSN()
          }),
          responseType: 'json',
          success: (response) => {
            const body = response.data || response
            if (body && (body.code === 0 || body.code === 200)) {
              console.log('[Signin] 签到已上报服务器')
              if (callback) callback(null, body.data)
            } else {
              const message = (body && body.message) || '签到上报失败'
              console.warn(`[Signin] ${message}`)
              if (callback) callback(new Error(message))
            }
          },
          fail: (data, code) => {
            console.warn(`[Signin] 签到上报请求失败: code=${code}`)
            if (callback) callback(new Error(`网络请求失败 code=${code}`))
          }
        })
      })
    })
  }

  /**
   * 解析老人 ID：优先取绑定信息，其次用 device_sn 查询设备列表
   */
  _resolveElderlyId(callback) {
    const bound = deviceBinding.getElderlyId()
    if (bound) {
      callback(bound)
      return
    }

    const deviceSn = deviceBinding.getDeviceSN()
    const header = cloudService.getAuthHeader()

    fetch.fetch({
      url: API_CONFIG.SERVICES.DEVICE + API_CONFIG.ENDPOINTS.V1.DEVICE_LIST + '?page=1&page_size=100',
      method: 'GET',
      header: header,
      responseType: 'json',
      success: (response) => {
        const body = response.data || response
        const data = body && body.data
        let list = []

        if (Array.isArray(data)) {
          list = data
        } else if (data) {
          list = data.list || data.items || data.records || []
        }

        let elderlyId = ''
        for (let i = 0; i < list.length; i++) {
          if (list[i] && list[i].device_sn === deviceSn) {
            elderlyId = list[i].elderly_id || ''
            break
          }
        }

        if (!elderlyId) {
          console.warn(`[Signin] 设备 ${deviceSn} 未绑定老人`)
        }
        callback(elderlyId)
      },
      fail: () => callback('')
    })
  }
}

export default new SigninService()
