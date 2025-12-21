package main

import (
	"fmt"
	"strconv"
	"strings"
)

const MIN_LATITUDE float64 = -85.05112878
const MAX_LATITUDE float64 = 85.05112878
const MIN_LONGITUDE float64 = -180
const MAX_LONGITUDE float64 = 180
const LATITUDE_RANGE float64 = MAX_LATITUDE - MIN_LATITUDE
const LONGITUDE_RANGE float64 = MAX_LONGITUDE - MIN_LONGITUDE

func valid_longitude(x float64) bool {
	return x >= MIN_LONGITUDE && x <= MAX_LONGITUDE
}
func valid_latitude(x float64) bool {
	return x >= MIN_LATITUDE && x <= MAX_LATITUDE
}

func spread_int32_to_int64(u int32) int64 {
	v := int64(u) & 0xFFFFFFFF

	v = (v | (v << 16)) & 0x0000FFFF0000FFFF
	v = (v | (v << 8)) & 0x00FF00FF00FF00FF
	v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v << 2)) & 0x3333333333333333
	v = (v | (v << 1)) & 0x5555555555555555

	return v
}

func get_zscore(latitude, longitude float64) int64 {
	normalized_latitude := int32((1 << 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE)
	normalized_longitude := int32((1 << 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE)
	x := spread_int32_to_int64(normalized_latitude)
	y := spread_int32_to_int64(normalized_longitude)

	return x | (y << 1)
}

func (server *Redis) handleGEOADD(client *Client, args []string) {
	key := args[0]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		resp := simple_err(
			fmt.Sprintf("failed to parse '%s' to float", args[1]),
		)

		client.conn.Write([]byte(resp))
		return
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		resp := simple_err(
			fmt.Sprintf("failed to parse '%s' to float", args[2]),
		)

		client.conn.Write([]byte(resp))
		return
	}
	if !valid_latitude(latitude) || !valid_longitude(longitude) {
		resp := simple_err(
			fmt.Sprintf("invalid latitude, longitude pair %s,%s", args[1], args[2]),
		)
		client.conn.Write([]byte(resp))
		return
	}

	score := get_zscore(latitude, longitude)
	score_str := strconv.FormatInt(score, 10)
	member := args[3]
	server.handleZADD(client, []string{key, score_str, member})
}

func is_geo_cmd(cmd string) bool {
	commands := []string{"GEOADD"}
	for _, command := range commands {
		if command == cmd {
			return true
		}
	}
	return false
}
func (server *Redis) handleGeo(client *Client, args []string) {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "GEOADD":
		server.handleGEOADD(client, args[1:])
	default:
		client.conn.Write([]byte(simple_err("shouldn't be here in geo")))
	}
}
