package kafkax

import "errors"

// permanentError 标记“重试也不会成功”的错误。
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent 包装一个错误，表示重试也不会成功，应当直接进死信。
//
// 典型场景是消息本身有问题（无法解析、引用的数据不存在、业务上不可能的组合）。
// 对这类错误原地重试只会让分区白白卡住 MaxAttempts 轮退避。err 为 nil 时返回 nil。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent 判断 err 链上是否有 Permanent 标记。
func IsPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}
