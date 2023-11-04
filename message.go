package socket

import (
	"encoding/json"
	"fmt"
	"github.com/go-tron/local-time"
	"github.com/go-tron/logger"
	"github.com/go-tron/socket/pb"
	"github.com/go-tron/types/fieldUtil"
	"github.com/thoas/go-funk"
	"strconv"
	"strings"
)

type messageIdGenerator interface {
	NextStringId() (string, error)
}

type messageConfig struct {
	logger      logger.Logger      //日志
	storage     messageStorage     //存储接口
	idGenerator messageIdGenerator //ID生成器
}

type WrappedMessage struct {
	*messageConfig
	*pb.Message
	uniqueChecked bool  //是否检查过唯一性
	arrived       int   //已送达
	attempts      uint  //发送次数
	loadErr       error //读取错误
}
type JsonMessageBody struct {
	Cmd     uint32      `protobuf:"bytes,1,opt,name=Cmd,proto3" json:"Cmd,omitempty"`
	Content interface{} `protobuf:"bytes,2,opt,name=Content,proto3" json:"Content,omitempty"`
}
type JsonMessage struct {
	Id       string           `protobuf:"bytes,1,opt,name=Id,proto3" json:"Id,omitempty"`
	Qom      uint32           `protobuf:"varint,2,opt,name=Qom,proto3" json:"Qom,omitempty"`
	Body     *JsonMessageBody `protobuf:"bytes,3,opt,name=Body,proto3" json:"Body,omitempty"`
	ExpireAt *localTime.Time  `protobuf:"bytes,4,opt,name=ExpireAt,proto3" json:"ExpireAt,omitempty"`
	ClientId string           `protobuf:"bytes,5,opt,name=ClientId,proto3" json:"ClientId,omitempty"`
	Ack      bool             `protobuf:"varint,6,opt,name=Ack,proto3" json:"Ack,omitempty"`
}

func (s *JsonMessage) String() string {
	if s.Body == nil {
		return ""
	}
	return fmt.Sprintf("[%s]%v", s.Body.Cmd, s.Body.Content)
}

var (
	all                     = []uint32{0, 1, 2, 3}
	save                    = []uint32{1, 2, 3}
	retry                   = []uint32{2, 3}
	persistence             = []uint32{3}
	checkRemoveAfterSend, _ = funk.DifferenceUInt32(save, retry)
)

// 检查qom是否有效
func (s *WrappedMessage) checkQom() bool {
	return funk.ContainsUInt32(all, s.Qom)
}

// 派发时暂存
func (s *WrappedMessage) checkNeedSave() bool {
	return funk.ContainsUInt32(save, s.Qom)
}

// 重试发送
func (s *WrappedMessage) checkNeedRetry() bool {
	return funk.ContainsUInt32(retry, s.Qom)
}

// 持久化
func (s *WrappedMessage) checkNeedPersistence() bool {
	return funk.ContainsUInt32(persistence, s.Qom)
}

// 发送后删除
func (s *WrappedMessage) checkRemoveAfterSend() bool {
	return funk.ContainsUInt32(checkRemoveAfterSend, s.Qom)
}

/*
	func (s *WrappedMessage) format() error {
		switch v := s.Body.(type) {
		case []byte:
			s.Body = string(v)
		case string:
			s.Body = v
		default:
			var err error
			body, err := json.Marshal(v)
			if err != nil {
				return err
			}
			s.Body = string(body)
		}
		return nil
	}
*/

func (s *WrappedMessage) generateId() (string, error) {
	if s.idGenerator == nil {
		return "", ErrorMessageIdUnset
	}
	return s.idGenerator.NextStringId()
}

func (s *WrappedMessage) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"id":       s.Id,
		"qom":      s.Qom,
		"body":     s.Body,
		"attempts": s.attempts,
	}
	if s.ExpireAt != nil {
		m["expireAt"] = s.ExpireAt
	}
	if s.checkNeedRetry() {
		m["ack"] = 1
	}
	return json.Marshal(m)
}

type messageOption func(message *WrappedMessage)

func messageWithClientId(val string) messageOption {
	return func(msg *WrappedMessage) {
		msg.ClientId = val
	}
}
func messageWithConfig(val *messageConfig) messageOption {
	return func(msg *WrappedMessage) {
		msg.messageConfig = val
	}
}

func initWrappedMessage(msg *WrappedMessage, opts ...messageOption) (err error) {
	if msg == nil {
		return ErrorBodyIsEmpty
	}
	for _, apply := range opts {
		if apply != nil {
			apply(msg)
		}
	}
	return nil
}
func initMessage(pm *pb.Message, opts ...messageOption) *WrappedMessage {
	var message = &WrappedMessage{
		Message: pm,
	}
	for _, apply := range opts {
		if apply != nil {
			apply(message)
		}
	}
	return message
}

func newMessage(pm *pb.Message, opts ...messageOption) (msg *WrappedMessage, err error) {
	msg = initMessage(pm, opts...)
	defer func() {
		if err != nil && msg.uniqueChecked {
			msg.uniqueRevert()
		}
		msg.log("new", "", err,
			logger.NewField("qom", msg.Qom),
			logger.NewField("body", msg.Body),
			logger.NewField("expireAt", msg.ExpireAt),
			logger.NewField("clientId", msg.ClientId),
		)
	}()

	if err := msg.validate(); err != nil {
		return msg, err
	}
	if msg.Id == "" {
		msg.Id, err = msg.generateId()
		if err != nil {
			return msg, err
		}
	} else {
		if err := msg.uniqueCheck(); err != nil {
			return msg, err
		}
	}

	/*if err := msg.format(); err != nil {
		return err
	}*/

	if err := msg.save(); err != nil {
		return msg, err
	}
	return msg, nil
}

func (s *WrappedMessage) log(event string, msg string, err error, v ...*logger.Field) {
	if s.logger == nil {
		return
	}
	fields := []*logger.Field{
		logger.NewField("event", event),
		logger.NewField("id", s.Id),
		logger.NewField("err", err),
	}
	fields = append(fields, v...)
	s.logger.Info(msg, fields...)
}

func (s *WrappedMessage) validate() error {
	if s.ClientId == "" {
		return ErrorClientIsEmpty
	}
	if s.ExpireAt != nil && localTime.FromTimestamppb(s.ExpireAt).Before(localTime.Now()) {
		return ErrorExpireAtInvalid
	}
	if !s.checkQom() {
		return ErrorQomMismatch
	}
	if fieldUtil.IsEmpty(s.Body) {
		return ErrorBodyIsEmpty
	}
	return nil
}

func (s *WrappedMessage) uniqueCheck() (err error) {
	if s.uniqueChecked {
		return nil
	}
	ok, err := s.storage.checkMessageUnique(s.Id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrorMessageIdExists
	}
	s.uniqueChecked = true
	return nil
}

func (s *WrappedMessage) uniqueRevert() (err error) {
	return s.storage.revertMessageUnique(s.Id)
}

func (s *WrappedMessage) save() (err error) {
	if !s.checkNeedSave() {
		return nil
	}
	return s.storage.saveMessage(s)
}

func (s *WrappedMessage) attemptSend(sendMaxAttempts uint) (err error) {
	s.attempts++
	defer func() {
		s.log("attemptSend", strconv.FormatUint(uint64(s.attempts), 10), err)
		if err == ErrorMessageExpired || err == ErrorMessageAttemptsLimit {
			if s.checkNeedSave() {
				s.remove(err.Error())
			}
		}
	}()

	if s.ExpireAt != nil && localTime.FromTimestamppb(s.ExpireAt).Before(localTime.Now()) {
		return ErrorMessageExpired
	}
	if s.attempts > sendMaxAttempts {
		return ErrorMessageAttemptsLimit
	}
	if s.arrived == 1 {
		return ErrorMessageHasArrived
	}

	if s.checkNeedRetry() {
		v, err := s.storage.attemptSendMessage(s.Id, sendMaxAttempts)
		if err != nil {
			if strings.HasPrefix(err.Error(), "not exists:") {
				return ErrorMessageRemoved
			} else if strings.HasPrefix(err.Error(), "has arrived:") {
				return ErrorMessageHasArrived
			} else if err.Error() == "attempts reach limit" {
				return ErrorMessageAttemptsLimit
			}
			return err
		}
		s.attempts = v
	}
	return nil
}

func (s *WrappedMessage) receive() (err error) {
	defer s.log("receive", "", err)
	s.arrived = 1
	return s.storage.receiveMessage(s, s.checkNeedPersistence())
}

func (s *WrappedMessage) remove(reason string) (err error) {
	defer s.log("remove", reason, err)
	return s.storage.removeMessage(s, s.checkNeedPersistence(), reason)
}
