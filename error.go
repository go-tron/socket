package socket

import "github.com/go-tron/base-error"

var ErrorMessageIdExists = baseError.New("90000", "消息ID已存在")
var ErrorClientOffline = baseError.New("90001", "客户端离线")
var ErrorClientConnectException = baseError.New("90002", "客户端连接异常")
var ErrorClientStatusException = baseError.New("90003", "客户端状态异常")
var ErrorClientNotFound = baseError.System("90004", "客户端Context不存在")
var ErrorClientIdUnset = baseError.System("90005", "客户端ID不存在")
var ErrorMessageIdUnset = baseError.New("90006", "消息ID不能为空")
var ErrorMessageExpired = baseError.New("90007", "消息已过期")
var ErrorMessageAttemptsLimit = baseError.New("90008", "消息已达到最大重试次数")
var ErrorMessageRemoved = baseError.New("90009", "消息已清理")
var ErrorMessageHasArrived = baseError.New("90010", "消息已送达")
var ErrorMessageHandlerUnset = baseError.New("90011", "未设置messageHandler")
var ErrorClientHasDisconnected = baseError.System("90012", "连接已断开")
var ErrorQomMismatch = baseError.New("90014", "qom不正确")
var ErrorBodyIsEmpty = baseError.New("90015", "body不能为空")
var ErrorClientIsEmpty = baseError.New("90016", "clientId不能为空")
var ErrorExpireAtInvalid = baseError.New("90017", "过期时间无效")
var ErrorGetNodeClient = baseError.Factory("90018", "获取节点地址失败:{}")
var ErrorGetClientStatus = baseError.Factory("90019", "获取客户端状态失败:{}")
var ErrorSameClientIdConnect = baseError.New("90020", "账号已在其他设备登录")
