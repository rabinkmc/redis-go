package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

func decode_length(reader *bufio.Reader) (uint32, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	msb := b >> 6
	switch msb {
	case 0:
		mask := uint8(0x3F)
		buf_size := (b & mask)
		return uint32(buf_size), nil
	case 1:
		mask := uint8(0x3F)
		size := uint16(b&mask) << 8
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		size = uint16(b&mask) | uint16(b)
		return uint32(size), nil
	case 2:
		buf := make([]byte, 4)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return 0, err
		}
		v := binary.BigEndian.Uint32(buf)
		return v, nil
	}
	return 0, nil
}

func read_str(reader *bufio.Reader, buf_size uint32) (string, error) {
	key_buf := make([]byte, buf_size)
	_, err := io.ReadFull(reader, key_buf)
	if err != nil {
		return "", err
	}
	return string(key_buf), nil
}

func (server *Redis) readRDB() {
	filename := filepath.Join(server.rdb_dir, server.dbfilename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open a file: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := []byte{}
	key_size := uint32(0)
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading from a file: %v", err)
		}
		buffer = append(buffer, b)
		if b == 0xFA {
			// header end
			// fmt.Println(string(buffer))
			buffer = buffer[:0]
		}
		if b == 0xFE {
			// index of the database
			server.db_index, err = decode_length(reader)
		}
		if b == 0xFB {
			server.key_size, err = decode_length(reader)
			if err != nil {
				log.Fatalf("Error decoding key size: %v", err)
			}
			server.exp_key_size, err = decode_length(reader)
			if err != nil {
				log.Fatalf("Error decoding expiry key size: %v", err)
			}
			break
		}
	}
	time_ms := uint64(0)
	i := 0
	for i < int(key_size) {
		value_type, err := reader.ReadByte()
		if err != nil {
			log.Fatalf("Error reading value type: %v", err)
		}
		if value_type == 0xFE {
			break
		}
		// key with expiry in sec
		if value_type == 0xFD {
			buf := make([]byte, 4)
			_, _ = io.ReadFull(reader, buf)
			time_ms = uint64(binary.LittleEndian.Uint32(buf)) * 1000
			fmt.Println("Probably just here", time_ms)
			continue
		} else if value_type == 0xFC {
			buf := make([]byte, 8)
			_, err = io.ReadFull(reader, buf)
			if err != nil {
				log.Fatalf("Error reading 8 bytes of unsigned long expiry time: %v", err)
			}
			time_ms = binary.LittleEndian.Uint64(buf)
			fmt.Println("Am I ever here", time_ms)
			continue
		}
		// read key value pair
		// assuming string
		buf_size, err := decode_length(reader)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		key_buf := make([]byte, buf_size)
		_, err = io.ReadFull(reader, key_buf)

		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		// value
		buf_size, err = decode_length(reader)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}

		value_buf := make([]byte, buf_size)
		_, err = io.ReadFull(reader, value_buf)

		if err != nil {
			log.Fatalf("Error: %v", err)
		}

		key, value := string(key_buf), string(value_buf)
		server.dict[key] = Entry{val: value}
		time_ms = 0
		i++
	}
}

func (server *Redis) handleKEYS() string {
	res := []string{}
	for key := range server.dict {
		res = append(res, key)
	}
	return encode_list(res)
}
