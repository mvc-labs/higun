package respond


type Message struct {
	Code           int         `json:"code"`
	Message        string      `json:"message"`
	ProcessingTime int64       `json:"processingTime"`
	Data           interface{} `json:"data"`
}

func RespSuccess(data interface{}, time int64) Message {
	return Message{
		Code:           HttpsCodeSuccess,
		Message:        RespMessageSuccess,
		ProcessingTime: time,
		Data:           data,
	}
}

func RespErr(err error, time int64, code int) Message {
	if code == 0 {
		code = HttpsCodeError
	}
	return Message{
		Code:           code,
		Message:        err.Error(),
		ProcessingTime: time,
		Data:           nil,
	}
}