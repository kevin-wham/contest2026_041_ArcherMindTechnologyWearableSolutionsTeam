/**
 * 报警服务
 * 负责 SOS 报警、跌倒报警、异常报警的处理
 * 匹配服务器 API：POST /api/v1/alarms/report
 */
import vibrator from '@system.vibrator'
import geolocation from '@system.geolocation'
import storage from '@system.storage'
import router from '@system.router'
import { ALARM_LEVELS, STORAGE_KEYS } from '../utils/constants'

export class AlarmService {
  constructor(uploadService) {
    this.uploadService = uploadService
    this.isSOSActive = false
    this.sosCallback = null
  }

  /**
   * 初始化报警服务
   */
  init(callbacks) {
    this.sosCallback = callbacks.onSOS
  }

  /**
   * 触发 SOS 报警
   * 匹配服务器 alarm_type: "SOS", severity: "P0"
   */
  triggerSOS(currentVitals) {
    if (this.isSOSActive) return

    this.isSOSActive = true
    console.log('[Alarm] SOS 报警触发')

    // 震动反馈
    this.startVibration('long')

    // 获取 GPS 位置
    this.getLocation((location) => {
      // 构建报警数据包
      const alarmPacket = this.uploadService.buildAlarmPacket(
        'sos',
        ALARM_LEVELS.P0,
        currentVitals,
        location,
        { trigger: 'MANUAL' }
      )

      // 保存报警记录到本地
      this.saveAlarmRecord(alarmPacket)

      // 跳转到 SOS 页面（由页面统一上报并显示"已报警/发送失败"结果）
      router.push({
        uri: '/pages/sos',
        params: {
          sosData: JSON.stringify(alarmPacket)
        }
      })

      // 通知回调
      if (this.sosCallback) {
        this.sosCallback(alarmPacket)
      }
    })
  }

  /**
   * 触发跌倒报警
   * 匹配服务器 alarm_type: "FALL", severity: "P0"
   */
  triggerFallAlarm(currentVitals) {
    console.log('[Alarm] 跌倒检测报警触发')

    // 震动反馈
    this.startVibration('long')

    // 获取 GPS 位置
    this.getLocation((location) => {
      // 构建报警数据包
      const alarmPacket = this.uploadService.buildAlarmPacket(
        'fall',
        ALARM_LEVELS.P0,
        currentVitals,
        location,
        {
          trigger: 'ALGORITHM',
          fall_probability: 0.87
        }
      )

      // 保存报警记录到本地
      this.saveAlarmRecord(alarmPacket)

      // 跳转到 SOS 页面（由页面统一上报并显示结果）
      router.push({
        uri: '/pages/sos',
        params: {
          sosData: JSON.stringify(alarmPacket)
        }
      })
    })
  }

  /**
   * 触发异常报警
   * 匹配服务器 alarm_type: "VITAL_ABNORMAL", severity: "P1"/"P2"
   */
  triggerAbnormalAlarm(reason, level, currentVitals) {
    console.log(`[Alarm] 异常报警: ${reason}, 级别: ${level}`)

    // 短震动提醒
    this.startVibration('short')

    // 构建报警数据包
    const alarmPacket = this.uploadService.buildAlarmPacket(
      'abnormal',
      level,
      currentVitals,
      null,
      { reason: reason }
    )

    // 保存报警记录到本地
    this.saveAlarmRecord(alarmPacket)

    // 上传报警数据到服务器
    this.uploadService.uploadAlarm(alarmPacket)
  }

  /**
   * 启动震动
   */
  startVibration(mode) {
    vibrator.vibrate({
      mode: mode,
      success: () => {},
      fail: (data, code) => {
        console.error(`[Alarm] 震动失败: code=${code}`)
      }
    })
  }

  /**
   * 停止震动
   */
  stopVibration() {
    vibrator.stop({
      success: () => {},
      fail: (data, code) => {
        console.error(`[Alarm] 停止震动失败: code=${code}`)
      }
    })
  }

  /**
   * 获取 GPS 位置
   */
  getLocation(callback) {
    geolocation.getLocation({
      success: (data) => {
        callback({
          latitude: data.latitude,
          longitude: data.longitude
        })
      },
      fail: (data, code) => {
        console.error(`[Alarm] 获取位置失败: code=${code}`)
        // GPS 失败时，报警仍可发送，位置字段为空
        callback(null)
      }
    })
  }

  /**
   * 保存报警记录到本地存储
   */
  saveAlarmRecord(alarmPacket) {
    storage.get({
      key: STORAGE_KEYS.SOS_RECORDS,
      success: (data) => {
        let records = []
        if (data) {
          try { records = JSON.parse(data) } catch (e) { records = [] }
        }
        records.push(alarmPacket)
        if (records.length > 100) records = records.slice(-100)
        storage.set({
          key: STORAGE_KEYS.SOS_RECORDS,
          value: JSON.stringify(records),
          success: () => {},
          fail: (data, code) => {
            console.error(`[Alarm] 保存报警记录失败: code=${code}`)
          }
        })
      },
      fail: () => {
        storage.set({
          key: STORAGE_KEYS.SOS_RECORDS,
          value: JSON.stringify([alarmPacket]),
          success: () => {},
          fail: () => {}
        })
      }
    })
  }

  /**
   * 取消 SOS 报警
   */
  cancelSOS() {
    this.isSOSActive = false
    this.stopVibration()
    console.log('[Alarm] SOS 已取消')
  }

  /**
   * 停止报警服务
   */
  stop() {
    this.cancelSOS()
  }
}

export default AlarmService
