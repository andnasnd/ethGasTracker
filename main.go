package main

import (
	"bytes"
	"context"
	"encoding/binary"
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

// FakeStdio can be used to fake stdin and capture stdout.
// Between creating a new FakeStdio and calling ReadAndRestore on it,
// code reading os.Stdin will get the contents of stdinText passed to New.
// Output to os.Stdout will be captured and returned from ReadAndRestore.
// FakeStdio is not reusable; don't attempt to use it after calling
// ReadAndRestore, but it should be safe to create a new FakeStdio.
type FakeStdio struct {
	origStdout   *os.File
	stdoutReader *os.File

	outCh chan []byte

	origStdin   *os.File
	stdinWriter *os.File
}

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
		// log.Printf("%s\n", key)
		// stub - run query here
	}
	return key
}

func insertRows(ctx context.Context, tx pgx.Tx, result *ethGasStationResponse) error {
	// log.Println("[Cluster] Inserting to ethGasStationData...")
	sec := time.Now().Unix()
	if _, err := tx.Exec(ctx,
		"INSERT INTO ethgasdata (ts, fast, fastest, safelow, average) VALUES ($1, $2, $3, $4, $5)", sec, result.Fast, result.Fastest, result.SafeLow, result.Average); err != nil {
		return err
	}
	return nil
}

func convertTo64(ar []byte) []float64 {
	newar := make([]float64, len(ar))
	var v byte
	var i int
	for i, v = range ar {
		newar[i] = float64(v)
	}
	return newar
}

func float64ToByte(f float64) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, f)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func graphData(result *ethGasStationResponse) error {

	var buf []byte

	res_f64 := float64ToByte(result.Average)

	// Pipe for stdin.
	//
	//                 ======
	//  w ------------->||||------> r
	// (stdinWriter)   ======      (os.Stdin)

	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return err
	}

	origStdin := os.Stdin
	os.Stdin = stdinReader

	_, err = stdinWriter.Write([]byte(buf))
	if err != nil {
		stdinWriter.Close()
		os.Stdin = origStdin
		return err
	}

	slice64 := convertTo64(res_f64)

	graph := asciigraph.Plot(slice64, asciigraph.Height(10), asciigraph.Width(40), asciigraph.Caption("Test Average Graph"))
	fmt.Println(graph)

	/*
		data := make([]float64, 0, 64)

		realTimeDataBuffer := int(40)

		s := bufio.NewScanner(os.Stdin)
		s.Split(bufio.ScanWords)

		nextFlushTime := time.Now()

		fps := float64(24)

		flushInterval := time.Duration(float64(time.Second) / fps)

		for s.Scan() {
			word := s.Text()
			p, err := strconv.ParseFloat(word, 64)
			if err != nil {
				log.Printf("ignore %q: error parsing data", word)
				continue
			}
			data = append(data, p)
			if realTimeDataBuffer > 0 && len(data) > realTimeDataBuffer {
				data = data[len(data)-realTimeDataBuffer:]
			}

			if currentTime := time.Now(); currentTime.After(nextFlushTime) || currentTime.Equal(nextFlushTime) {
				plot := asciigraph.Plot(data,
					asciigraph.Height(int(10)),
					asciigraph.Width(int(40)),
					asciigraph.Caption("ethGas Average (ms)"))
				asciigraph.Clear()
				fmt.Println(plot)
				nextFlushTime = time.Now().Add(flushInterval)
			}
		}
	*/

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
	// slug = "https://data-api.defipulse.com/api/v1/egs/api/ethgasAPI.json?api-key=" + key
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

	// graphData(result)

	err = crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return insertRows(context.Background(), tx, result)
	})
	if err == nil {
		// log.Println("[Cluster] Insertion to ethGasStationData success")
	} else {
		log.Fatal("error: ", err)
	}

	printGraphRT(conn, data)
}

func connectToCluster() (conn *pgx.Conn, invalidtype error) {
	connstring := "postgresql://anand:71-856laTUj5TZMe@free-tier4.aws-us-west-2.cockroachlabs.cloud:26257/test?sslmode=verify-full&sslrootcert=$HOME/.postgresql/root.crt&options=--cluster%3Dthorny-jaguar-1926"

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

	// for {
	// time.Sleep(time.Second * 5)
	// printGraphRT(conn, data)
	// subroutine(conn, data)
	// }

	s := gocron.NewScheduler()
	s.Every(1).Seconds().Do(subroutine, conn, data)
	<-s.Start()

}
