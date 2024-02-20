package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

func getURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
// 	fmt.Println("Got resp:", resp)
// 	fmt.Println("Got err:", err)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
//     bodyString := string(body)
//     fmt.Println(bodyString)
// 	fmt.Println("Got err 2:", err)
	if err != nil {
		return nil, err
	}
	return body, nil
}

const (
	url_btse     = "https://api.btse.com/futures/api/v2.1/orderbook?symbol=ETHPFC&depth=10"
	url_whitebit = "https://whitebit.com/api/v4/public/orderbook/BTC_PERP?limit=10"
)

var btse_pings []float64
var whitebit_pings []float64

func calculateAverage(pings []float64) float64 {
	var sum float64
	for _, ping := range pings {
		sum += ping
	}
	return sum / float64(len(pings))
}

func main() {
	for {

		start := time.Now()
		_, err := getURL(url_btse)
		if err != nil {
			fmt.Println("Error fetching URL:", err)
			continue
		}
		elapsed := time.Since(start).Seconds()
		btse_pings = append(btse_pings, elapsed)
// 		fmt.Printf("Time taken for %s: %.2fs\n", url_btse, elapsed)

		start = time.Now()
		_, err = getURL(url_whitebit)
		if err != nil {
			fmt.Println("Error fetching URL:", err)
			continue
		}
		elapsed = time.Since(start).Seconds()
		whitebit_pings = append(whitebit_pings, elapsed)
// 		fmt.Printf("Time taken for %s: %.2fs\n", url_whitebit, elapsed)

		if len(btse_pings) > 0 {
			fmt.Printf("Average time for BTSE: %.4fs\n", calculateAverage(btse_pings))
		}
		if len(whitebit_pings) > 0 {
			fmt.Printf("Average time for Whitebit: %.4fs\n", calculateAverage(whitebit_pings))
		}

		time.Sleep(1 * time.Second)
	}
}
