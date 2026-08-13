// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package terror is a trimmed-down fork of TiDB's terror package. It retains
// just enough to define the parser's typed errors: an error class registry
// and constructors that attach MySQL error codes to *errors.Error values.
package terror

import (
	"fmt"
	"strconv"

	"github.com/pingcap/errors"

	"github.com/block/spirit/pkg/parser/mysql"
)

// ErrCode represents a specific error type in an error class.
// Same error code can be used in different error classes.
type ErrCode int

// ErrClass represents a class of errors.
type ErrClass int

// Error implements error interface.
type Error = errors.Error

// Error classes used by the parser.
var (
	ClassDDL    = RegisterErrorClass(2, "ddl")
	ClassParser = RegisterErrorClass(11, "parser")
	ClassTypes  = RegisterErrorClass(20, "types")
)

var errClass2Desc = make(map[ErrClass]string)

// RegisterErrorClass registers new error class for terror.
func RegisterErrorClass(classCode int, desc string) ErrClass {
	errClass := ErrClass(classCode)
	if _, exists := errClass2Desc[errClass]; exists {
		panic(fmt.Sprintf("duplicate register ClassCode %d - %s", classCode, desc))
	}
	errClass2Desc[errClass] = desc
	return errClass
}

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	if s, exists := errClass2Desc[ec]; exists {
		return s
	}
	return strconv.Itoa(int(ec))
}

// NewStdErr defines an *Error with an error code, an error
// message and workaround to create standard error.
func (ec ErrClass) NewStdErr(code ErrCode, message *mysql.ErrMessage) *Error {
	rfcCode := fmt.Sprintf("%s:%d", errClass2Desc[ec], code)
	err := errors.Normalize(
		message.Raw, errors.RedactArgs(message.RedactArgPos),
		errors.MySQLErrorCode(int(code)), errors.RFCCodeText(rfcCode),
	)
	return err
}

// NewStd creates an *Error using the standard message for the error code.
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code ErrCode) *Error {
	return ec.NewStdErr(code, mysql.MySQLErrName[uint16(code)])
}

// ErrorEqual returns a boolean indicating whether err1 is equal to err2.
func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}

	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	te1, ok1 := e1.(*Error)
	te2, ok2 := e2.(*Error)
	if ok1 && ok2 {
		return te1.RFCCode() == te2.RFCCode()
	}

	return e1.Error() == e2.Error()
}
