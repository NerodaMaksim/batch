package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const nodeUrl = "https://mainnet.infura.io/v3/ddf65afc6260497b85a339c0dfa371ee"

type ReqTarget string

const (
	NodeRequest    ReqTarget = "node"
	IndexerRequest ReqTarget = "indexer"
)

type BatchElem struct {
	request map[string]json.RawMessage
	id      int
}

type Batch struct {
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

func (b *Batch) SendBatch(nodeUrl, indexerUrl string) (io.ReadCloser, error) {
	var wg sync.WaitGroup
	var nodeResponse *bytes.Buffer
	var indexerResponse *bytes.Buffer
	wg.Add(1)
	errChan := make(chan error)
	reqStartTime := time.Now()
	go func(errChan chan error) {
		defer wg.Done()
		marshalingTime := time.Now()
		nodeResponse = new(bytes.Buffer)
		nodeBody := bytes.NewBuffer([]byte("["))
		for i, req := range b.nodeReq {
			data, err := json.Marshal(req.request)
			if err != nil {
				errChan <- err
				return
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
		response, err := http.DefaultClient.Post(nodeUrl, "application/json", nodeBody)
		if err != nil {
			errChan <- err
			return
		}

		io.Copy(nodeResponse, response.Body)
	}(errChan)

	wg.Add(1)

	go func(errChan chan error) {
		defer wg.Done()
		marshalingTime := time.Now()
		indexerResponse = new(bytes.Buffer)
		indexerBody := bytes.NewBuffer([]byte("["))
		for i, req := range b.indexerReq {
			data, err := json.Marshal(req.request)
			if err != nil {
				errChan <- err
				return
			}
			comma := ""
			if i != len(b.indexerReq)-1 {
				comma = ","
			}
			indexerBody.WriteString(string(data) + comma)
		}
		indexerBody.WriteString("]")
		//
		fmt.Println("Marshaling time", time.Since(marshalingTime))
		fmt.Println(indexerBody.String())
		response, err := http.DefaultClient.Post(indexerUrl, "application/json", indexerBody)
		if err != nil {
			errChan <- err
			return
		}

		io.Copy(indexerResponse, response.Body)
	}(errChan)

	wg.Wait()
	fmt.Println("Request duration", time.Since(reqStartTime))
	if len(errChan) != 0 {
		return nil, <-errChan
	}

	// Set response into fixed order

	return io.NopCloser(b.orderResponses(splitIntoResponses(nodeResponse), splitIntoResponses(indexerResponse))), nil
}

func main() {
	startTime := time.Now()
	var a []map[string]json.RawMessage
	var b map[string]json.RawMessage
	//line := []byte(`{"a": "b", "c": "d"}`)
	//line := []byte(`[{"a": "b", "c": "d"}, {"e": "b", "f": "d"}]`)
	line := []byte(`[{"method":"eth_syncing","params":[],"id":2,"jsonrpc":"2.0"},{"jsonrpc":"2.0","id":696969,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]}]`)
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

	// Batch request, needs additional proccessing
	// fmt.Println(a)

	batch := &Batch{order: make(map[int]ReqTarget)}
	for i, obj := range a {
		fmt.Println(string(obj["method"]))
		if string(obj["method"]) == `"eth_getLogs"` {
			batch.indexerReq = append(batch.indexerReq, &BatchElem{request: obj, id: i})
			batch.order[i] = IndexerRequest
		} else {
			batch.nodeReq = append(batch.nodeReq, &BatchElem{request: obj, id: i})
			batch.order[i] = NodeRequest
		}

	}

	body, err := batch.SendBatch(nodeUrl, nodeUrl)
	if err != nil {
		panic(err)
	}
	_ = body
	// bodyBytes, err := io.ReadAll(body)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(bodyBytes))
	fmt.Println(time.Since(startTime))
}
