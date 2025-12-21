package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	MIN_LATITUDE    = -85.05112878
	MAX_LATITUDE    = 85.05112878
	MIN_LONGITUDE   = -180
	MAX_LONGITUDE   = 180
	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
)

func valid_longitude(x float64) bool {
	return x >= MIN_LONGITUDE && x <= MAX_LONGITUDE
}
func valid_latitude(x float64) bool {
	return x >= MIN_LATITUDE && x <= MAX_LATITUDE
}

func spread_int32_to_int64(u uint32) uint64 {
	v := uint64(u) & 0xFFFFFFFF

	v = (v | (v << 16)) & 0x0000FFFF0000FFFF
	v = (v | (v << 8)) & 0x00FF00FF00FF00FF
	v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v << 2)) & 0x3333333333333333
	v = (v | (v << 1)) & 0x5555555555555555

	return v
}

func encode_pos(latitude, longitude float64) uint64 {
	normalized_latitude := uint32((1 << 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE)
	normalized_longitude := uint32((1 << 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE)
	x := spread_int32_to_int64(normalized_latitude)
	y := spread_int32_to_int64(normalized_longitude)

	return x | (y << 1)
}

func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) (float64, float64) {
	// Calculate the grid boundaries
	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/math.Pow(2, 26))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/math.Pow(2, 26))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/math.Pow(2, 26))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/math.Pow(2, 26))

	// Calculate the center point of the grid cell
	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2

	return longitude, latitude
}

func decode_geocode(zscore uint64) (float64, float64) {
	y := zscore >> 1
	x := zscore
	latitudeNumber := compactInt64ToInt32(x)
	longitudeNumber := compactInt64ToInt32(y)
	return convertGridNumbersToCoordinates(latitudeNumber, longitudeNumber)
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

	score := encode_pos(latitude, longitude)
	score_str := strconv.FormatUint(score, 10)
	member := args[3]
	server.handleZADD(client, []string{key, score_str, member})
}
func (server *Redis) handleGEOPOS(client *Client, args []string) {
	key := args[0]
	places := args[1:]
	entry, _ := server.dict[key]
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(places)))
	n := len(entry.zset)
	for _, place := range places {
		if n == 0 {
			b.WriteString(NULL_ARRAY)
			continue
		}
		idx := entry.find_znode(place)
		if idx == -1 {
			b.WriteString(NULL_ARRAY)
			continue
		}
		lat, long := decode_geocode(uint64(entry.zset[idx].score))
		b.WriteString(encode_list([]string{
			strconv.FormatFloat(lat, 'f', -1, 64),
			strconv.FormatFloat(long, 'f', -1, 64),
		}))
	}
	client.conn.Write([]byte(b.String()))

}

func is_geo_cmd(cmd string) bool {
	commands := []string{"GEOADD", "GEOPOS"}
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
	case "GEOPOS":
		server.handleGEOPOS(client, args[1:])
	default:
		client.conn.Write([]byte(simple_err("shouldn't be here in geo")))
	}
}
