package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const nodeUrl = "https://mainnet.infura.io/v3/ddf65afc6260497b85a339c0dfa371ee"

type ReqTarget string

var requestDuration time.Duration

const (
	NodeRequest    ReqTarget = "node"
	IndexerRequest ReqTarget = "indexer"
)

type BatchElem struct {
	request map[string]json.RawMessage
	id      int
}

type Batch struct {
	req        []*BatchElem
	nodeReq    []*BatchElem
	indexerReq []*BatchElem
	order      map[int]ReqTarget
}

func splitIntoResponses(resp *bytes.Buffer) []*bytes.Buffer {
	resps := []*bytes.Buffer{}
	for {
		openedZippers := 0
		closedZippers := 0
		entryChar := resp.Next(1)
		if len(entryChar) == 0 {
			break
		}
		if entryChar[0] == '{' && openedZippers == 0 && closedZippers == 0 {
			singleResp := bytes.NewBuffer(entryChar)
			openedZippers++
			stopSearch := false
			for {
				if stopSearch {
					break
				}
				char := resp.Next(1)
				if len(char) == 0 {
					break
				}
				switch char[0] {
				case '{':
					openedZippers++
					singleResp.Write(char)
				case '}':
					closedZippers++
					singleResp.Write(char)
					if closedZippers == openedZippers {
						resps = append(resps, singleResp)
						closedZippers = 0
						openedZippers = 0
						stopSearch = true
						break
					}
				default:
					singleResp.Write(char)
				}
			}
		}
	}
	return resps
}

func (b *Batch) orderResponses(nodeResponse, indexerResponse []*bytes.Buffer) *bytes.Buffer {
	orderingTime := time.Now()
	orderedResponses := bytes.NewBufferString("[")
	idxRespId := 0
	nodeRespId := 0
	if len(nodeResponse) != len(b.nodeReq) || len(indexerResponse) != len(b.indexerReq) {
		return nil
	}
	fmt.Println(b.order)
	for i := 0; i < len(b.order); i++ {
		if b.order[i] == IndexerRequest {
			orderedResponses.ReadFrom(indexerResponse[idxRespId])
			if i != len(b.order)-1 {
				orderedResponses.WriteString(",")
			}
			idxRespId++
			continue
		}
		orderedResponses.ReadFrom(nodeResponse[nodeRespId])
		if i != len(b.order)-1 {
			orderedResponses.WriteString(",")
		}
		nodeRespId++
	}
	orderedResponses.WriteString("]")
	fmt.Println("Ordeering time", time.Since(orderingTime))
	return orderedResponses
}

func (b *Batch) SplittBatch(batch []*BatchElem, Url string) *bytes.Buffer {

	//errChan := make(chan error)
	//reqStartTime := time.Now()
	var Response *bytes.Buffer
	marshalingTime := time.Now()
	Response = new(bytes.Buffer)
	nodeBody := bytes.NewBuffer([]byte("["))
	for i, req := range batch {
		data, err := json.Marshal(req.request)
		if err != nil {
			panic(err)
		}
		comma := ""
		if i != len(b.nodeReq)-1 {
			comma = ","
		}
		nodeBody.WriteString(string(data) + comma)
	}
	nodeBody.WriteString("]")
	//
	fmt.Println("Marshaling time", time.Since(marshalingTime))
	fmt.Println(nodeBody.String())
	response, err := http.DefaultClient.Post(Url, "application/json", nodeBody)
	if err != nil {
		panic(err)
	}

	_, err = io.Copy(Response, response.Body)
	if err != nil {
		panic(err)
	}

	return Response
}

func (b *Batch) SendBatch(nodeUrl, indexerUrl string) (io.ReadCloser, error) {
	var wg sync.WaitGroup

	wg.Add(1)
	errChan := make(chan error)
	reqStartTime := time.Now()

	wg.Wait()
	requestDuration = time.Since(reqStartTime)
	fmt.Println("Request duration", requestDuration)
	if len(errChan) != 0 {
		return nil, <-errChan
	}

	// Set response into fixed order
	var (
		separateResponsesFromNode    []*bytes.Buffer
		separateResponsesFromIndexer []*bytes.Buffer
		responseFromNode             *bytes.Buffer
		responsefromIndexer          *bytes.Buffer
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		responseFromNode = b.SplittBatch(b.nodeReq, nodeUrl)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		responsefromIndexer = b.SplittBatch(b.indexerReq, indexerUrl)
	}()
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		separateResponsesFromNode = splitIntoResponses(responseFromNode)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		separateResponsesFromIndexer = splitIntoResponses(responsefromIndexer)
	}()

	wg.Wait()

	return io.NopCloser(b.orderResponses(separateResponsesFromNode, separateResponsesFromIndexer)), nil
}

func ForIndexer(arg []byte) bool {
	isIndexer := false
	line := string(arg)

	if strings.Contains(line, "eth_getlogs") {
		isIndexer = true
	}
	return isIndexer
}

func (b *Batch) NewBatch(a []map[string]json.RawMessage, url string) (*Batch, error) {

	batch := &Batch{order: make(map[int]ReqTarget)}
	for i, obj := range a {
		fmt.Println(string(obj["method"]))
		if string(obj["method"]) == "eth_getLogs" {
			batch.indexerReq = append(batch.indexerReq, &BatchElem{request: obj, id: i})
			batch.order[i] = IndexerRequest
		} else {
			batch.nodeReq = append(batch.nodeReq, &BatchElem{request: obj, id: i})
			batch.order[i] = NodeRequest
		}

	}
	body, err := batch.SendBatch(url, url)
	if err != nil {
		panic(err)
	}
	_ = body

	return batch, nil
}

func main() {
	var batch *Batch
	startTime := time.Now()
	var a []map[string]json.RawMessage
	var b map[string]json.RawMessage
	//line := []byte(`{"a": "b", "c": "d"}`)
	//line := []byte(`[{"a": "b", "c": "d"}, {"e": "b", "f": "d"}]`)
	line := []byte(`[{"method":"eth_syncing","params":[],"id":2,"jsonrpc":"2.0"},{"jsonrpc":"2.0","id":696969,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]}]`)
	// line := []byte(`[{"method":"eth_syncing","params":[],"id":1,"jsonrpc":"2.0"},{"method":"eth_syncing","params":[],"id":2,"jsonrpc":"2.0"}, {"jsonrpc":"2.0","id":3,"method":"eth_getLogs","params":[{"toBlock":"0x1","fromBlock":"0x0"}]}, {"method":"eth_syncing","params":[],"id":4,"jsonrpc":"2.0"}]`)
	err := json.Unmarshal(line, &a)
	if err != nil {
		err := json.Unmarshal(line, &b)
		if err != nil {
			panic(err)
		}
		// Common request
		fmt.Println(b)
		return
	}
	if ForIndexer(line) == true {
		batch.NewBatch(a, nodeUrl)
		if err != nil {
			panic(err)
		}
	}
	_, err = batch.NewBatch(a, nodeUrl)
	if err != nil {
		panic(err)
	}
	// Batch request, needs additional processing
	// fmt.Println(a)

	// bodyBytes, err := io.ReadAll(body)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(bodyBytes))
	allDuration := time.Since(startTime)
	fmt.Println("All duration time", allDuration)
	fmt.Println("Delta", allDuration-requestDuration)
}
