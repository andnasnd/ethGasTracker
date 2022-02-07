package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/guptarohit/asciigraph"
	"github.com/jackc/pgx/v4"
	"github.com/jasonlvhit/gocron"
)

type ethGasStationResponse struct {
	Fast       float64 `json:"fast"`
	Fastest    float64 `json:"fastest"`
	SafeLow    float64 `json:"safeLow"`
	Average    float64 `json:"average"`
	Block_Time float64 `json:"block_time"`
	BlockNum   float64 `json:"blockNum"`
	Speed      float64 `json:"speed"`
}

func getAPIKey(conn *pgx.Conn) string {
	var key string
	row, err := conn.Query(context.Background(), "SELECT key FROM api_key_store WHERE name = 'data.defipulse';")
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	for row.Next() {
		if err := row.Scan(&key); err != nil {
			log.Fatal(err)
		}
	}
	return key
}

func insertRows(ctx context.Context, tx pgx.Tx, result *ethGasStationResponse) error {
	sec := time.Now().Unix()
	if _, err := tx.Exec(ctx,
		"INSERT INTO ethgasdata (ts, fast, fastest, safelow, average) VALUES ($1, $2, $3, $4, $5)", sec, result.Fast, result.Fastest, result.SafeLow, result.Average); err != nil {
		return err
	}
	return nil
}

func queryRows(conn *pgx.Conn, data []float64) (out []float64, e error) {
	rows, err := conn.Query(context.Background(), "SELECT fast FROM ethgasdata")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var fast float64
		if err := rows.Scan(&fast); err != nil {
			log.Fatal(err)
			return nil, err
		}
		data = append(data, fast)
	}
	return data, nil
}

func GETRequest(key string) (*ethGasStationResponse, error) {
	slug := "https://ethgasstation.info/json/ethgasAPI.json"
	resp, err := http.Get(slug)
	if err != nil {
		log.Fatal(err)
	}
	var result ethGasStationResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return &result, nil
}

func subroutine(conn *pgx.Conn, data []float64) {
	result, err := GETRequest(getAPIKey(conn))
	if err != nil {
		log.Fatal(err)
	}

	err = crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return insertRows(context.Background(), tx, result)
	})
	if err != nil {
		log.Fatal("error: ", err)
	}

	printGraphRT(conn, data)
}

func connectToCluster() (conn *pgx.Conn, invalidtype error) {
	connstring := "REDACTED"

	config, err := pgx.ParseConfig(os.ExpandEnv(connstring))
	if err != nil {
		log.Fatal("error configuring the database: ", err)
	}
	conn, err = pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	return conn, nil
}

func printGraphRT(conn *pgx.Conn, data []float64) {

	data, err := queryRows(conn, data)
	if err != nil {
		log.Fatal(err)
	}

	nextFlushTime := time.Now()
	realTimeDataBuffer := int(100)
	fps := float64(24)
	flushInterval := time.Duration(float64(time.Second) / fps)

	if realTimeDataBuffer > 0 && len(data) > realTimeDataBuffer {
		data = data[len(data)-realTimeDataBuffer:]
	}

	if currentTime := time.Now(); currentTime.After(nextFlushTime) || currentTime.Equal(nextFlushTime) {
		plot := asciigraph.Plot(data,
			asciigraph.Height(15),
			asciigraph.Width(100),
			asciigraph.Offset(3),
			asciigraph.Precision(2),
			asciigraph.Caption("realtime eth gas price over time (Gwei per gas)"))
		asciigraph.Clear()
		fmt.Println(plot)
		fmt.Println("\nData = ", data)
		nextFlushTime = time.Now().Add(flushInterval)
	}
}

func main() {

	conn, err := connectToCluster()
	if err == nil {
		log.Println("🤖 successfully connected to cluster 🤖")
	}

	data := make([]float64, 0, 64)

	defer conn.Close(context.Background())

	s := gocron.NewScheduler()
	s.Every(1).Seconds().Do(subroutine, conn, data)
	<-s.Start()

}
