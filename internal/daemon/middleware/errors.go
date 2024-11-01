package middleware

func NewHttpError(statusCode int, err error) *HttpError {
	return &HttpError{err: err, statusCode: statusCode}
}

type HttpError struct {
	err        error
	statusCode int
}

func (self *HttpError) StatusCode() int { return self.statusCode }

func (self *HttpError) Error() string { return self.err.Error() }

func (self *HttpError) Unwrap() error { return self.err }
