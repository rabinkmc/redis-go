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

	return latitude, longitude
}

func decode_geocode(zscore uint64) (float64, float64) {
	y := zscore >> 1
	x := zscore
	latitudeNumber := compactInt64ToInt32(x)
	longitudeNumber := compactInt64ToInt32(y)
	return convertGridNumbersToCoordinates(latitudeNumber, longitudeNumber)
}

func haversine(θ float64) float64 {
	return .5 * (1 - math.Cos(θ))
}

type pos_radian struct {
	lat  float64
	long float64
}

func deg_to_radian(lat, lon float64) pos_radian {
	return pos_radian{lat * math.Pi / 180, lon * math.Pi / 180}
}

func hsDist(p1, p2 pos_radian) float64 {
	const rEarth = 6372797.560856 //m
	return 2 * rEarth * math.Asin(math.Sqrt(haversine(p2.lat-p1.lat)+
		math.Cos(p1.lat)*math.Cos(p2.lat)*haversine(p2.long-p1.long)))
}

func HandleGEOADD(server *Redis, cmd Command) {
	if len(cmd.Args) < 5 {
		cmd.Client.WriteErr("Invalid usage: GEOADD key long lat member")
		return
	}

	client := cmd.Client
	key := cmd.Args[1]
	longitude, err := strconv.ParseFloat(cmd.Args[2], 64)
	if err != nil {
		resp := simple_err(
			fmt.Sprintf("failed to parse '%s' to float", cmd.Args[2]),
		)

		client.conn.Write([]byte(resp))
		return
	}
	latitude, err := strconv.ParseFloat(cmd.Args[3], 64)
	if err != nil {
		resp := simple_err(
			fmt.Sprintf("failed to parse '%s' to float", cmd.Args[3]),
		)

		client.conn.Write([]byte(resp))
		return
	}
	if !valid_latitude(latitude) || !valid_longitude(longitude) {
		resp := simple_err(
			fmt.Sprintf("invalid latitude, longitude pair %s,%s", cmd.Args[2], cmd.Args[3]),
		)
		client.conn.Write([]byte(resp))
		return
	}

	score := encode_pos(latitude, longitude)
	score_str := strconv.FormatUint(score, 10)
	member := cmd.Args[4]
	command := Command{
		Client: client,
		Args:   []string{"ZADD", key, score_str, member},
	}
	HandleZADD(server, command)
}

func HandleGEOPOS(server *Redis, cmd Command) {
	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr("Invalid usage: GEOPOS key place1 [place...]")
		return
	}
	client := cmd.Client
	key := cmd.Args[1]
	places := cmd.Args[2:]
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
			strconv.FormatFloat(long, 'f', -1, 64),
			strconv.FormatFloat(lat, 'f', -1, 64),
		}))
	}
	client.conn.Write([]byte(b.String()))
}

func HandleGEODIST(server *Redis, cmd Command) {
	if len(cmd.Args) < 4 {
		cmd.Client.WriteErr("Invalid usage: GEODIST key place1 place2")
		return
	}
	key := cmd.Args[1]
	place1 := cmd.Args[2]
	place2 := cmd.Args[3]
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		cmd.Client.WriteErr("No place found")
		return
	}
	id1 := entry.find_znode(place1)
	if id1 == -1 {
		cmd.Client.WriteErr(fmt.Sprintf("'%s' not found", place1))
		return
	}

	id2 := entry.find_znode(place2)
	if id2 == -1 {
		cmd.Client.WriteErr(fmt.Sprintf("'%s' not found", place2))
		return
	}
	lat1, long1 := decode_geocode(uint64(entry.zset[id1].score))
	lat2, long2 := decode_geocode(uint64(entry.zset[id2].score))
	dist := hsDist(deg_to_radian(lat1, long1), deg_to_radian(lat2, long2))
	resp := strconv.FormatFloat(dist, 'f', -1, 64)
	cmd.Client.WriteBulkString(resp)
}

func HandleGEOSEARCH(server *Redis, cmd Command) {
	client := cmd.Client
	key := cmd.Args[1]
	// args[1] => FROMLONLAT
	longitude, err := strconv.ParseFloat(cmd.Args[2], 64)
	if err != nil {
		resp := fmt.Sprintf("failed to parse '%s' to float", cmd.Args[2])
		cmd.Client.WriteErr(resp)
		return
	}
	latitude, err := strconv.ParseFloat(cmd.Args[3], 64)
	if err != nil {
		resp := fmt.Sprintf("failed to parse '%s' to float", cmd.Args[3])
		cmd.Client.WriteErr(resp)
		return
	}
	// args[4] BYRADIUS
	within, err := strconv.ParseFloat(cmd.Args[4], 64)
	// args[6] unit will be m for us
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		// resp := "No place found for the given key"
		client.conn.Write([]byte(EMPTY_ARRAY))
		return
	}
	res := []string{}
	for _, item := range entry.zset {
		lat1, long1 := decode_geocode(uint64(item.score))
		dist := hsDist(deg_to_radian(lat1, long1), deg_to_radian(latitude, longitude))
		if dist <= within {
			res = append(res, item.member)
		}
	}
	client.conn.Write([]byte(encode_list(res)))
}

func is_geo_cmd(cmd Command) bool {
	cmdName := cmd.Args[0]
	commands := []string{"GEOADD", "GEOPOS", "GEODIST", "GEOSEARCH"}
	for _, command := range commands {
		if command == cmdName {
			return true
		}
	}
	return false
}

func HandleGeo(server *Redis, cmd Command) {
	cmdName := strings.ToUpper(cmd.Args[0])
	switch cmdName {
	case "GEOADD":
		HandleGEOADD(server, cmd)
	case "GEOPOS":
		HandleGEOPOS(server, cmd)
	case "GEODIST":
		HandleGEODIST(server, cmd)
	case "GEOSEARCH":
		HandleGEOSEARCH(server, cmd)
	default:
		cmd.Client.WriteErr("Invalid geo command")
	}
}
