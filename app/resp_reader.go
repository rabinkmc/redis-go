package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RespReader struct {
	reader *bufio.Reader
}

func NewRespReader(rd io.Reader) *RespReader {
	return &RespReader{
		reader: bufio.NewReader(rd),
	}
}

func parse_resp_arr(reader *bufio.Reader, arr_size int) ([]string, error) {
	line, err := reader.ReadString('\n')
	args := []string{}
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		return nil, fmt.Errorf(
			"Expected array prefix '*' got '%q'",
			line[0],
		)
	}
	line = strings.TrimSuffix(line, "\r\n")
	arr_size, err = strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf(
			"Invaid arg size: %v\n",
			err,
		)
	}
	for i := 0; i < arr_size; i++ {
		size_str, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf(
				"Error reading from connection : %v",
				err,
			)
		}
		size_str = strings.TrimSuffix(size_str, "\r\n")
		size, err := strconv.Atoi(size_str[1:])
		if err != nil {
			return nil, fmt.Errorf(
				"Error parsing integer: %v",
				err,
			)
		}
		buf := make([]byte, size)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, fmt.Errorf(
				"Error reading buffer of size: %d",
				size,
			)
		}
		args = append(args, string(buf))
		_, err = reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf(
				"Error reading trailing CRLF: %v",
				err,
			)
		}
	}
	if len(args) == 0 {
		return nil, errors.New("No args provided.")
	}
	return args, nil
}

func (r *RespReader) ReadCommand() ([]string, error) {
	line, err := r.reader.ReadString('\n')
	args := []string{}
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		return nil, fmt.Errorf(
			"Expected array prefix '*' got '%q'",
			line[0],
		)
	}
	line = strings.TrimSuffix(line, "\r\n")
	arr_size, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf(
			"Invaid arg size: %v\n",
			err,
		)
	}
	args, err = r.ParseRespArr(arr_size)
	if err != nil {
		return nil, err
	}
	return args, nil
}

func (r *RespReader) ParseRespArr(arr_size int) ([]string, error) {
	args := []string{}
	for i := 0; i < arr_size; i++ {
		size_str, err := r.reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf(
				"Error reading from connection : %v",
				err,
			)
		}
		size_str = strings.TrimSuffix(size_str, "\r\n")
		size, err := strconv.Atoi(size_str[1:])
		if err != nil {
			return nil, fmt.Errorf(
				"Error parsing integer: %v",
				err,
			)
		}
		buf := make([]byte, size)
		_, err = io.ReadFull(r.reader, buf)
		if err != nil {
			return nil, fmt.Errorf(
				"Error reading buffer of size: %d",
				size,
			)
		}
		args = append(args, string(buf))
		_, err = r.reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf(
				"Error reading trailing CRLF: %v",
				err,
			)
		}
	}
	if len(args) == 0 {
		return nil, errors.New("No args provided.")
	}
	return args, nil
}
