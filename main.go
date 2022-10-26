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

type ReqTarget string

var requestDuration time.Duration

const (
	NodeRequest    ReqTarget = "node"
	IndexerRequest ReqTarget = "indexer"
	getLogsMethod  string    = "eth_getLogs"
	nodeUrl                  = "https://mainnet.infura.io/v3/ddf65afc6260497b85a339c0dfa371ee"
	indexerUrl               = "https://mainnet.infura.io/v3/0ae14f9415c34ea49c7e0012e5ff1248"
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

func (b *Batch) SplitBatch(batch []*BatchElem, Url string, resp *bytes.Buffer) error {

	//errChan := make(chan error)
	//reqStartTime := time.Now()
	marshalingTime := time.Now()
	nodeBody := bytes.NewBuffer([]byte("["))
	for i, req := range batch {
		data, err := json.Marshal(req.request)
		if err != nil {
			return err
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
	reqStartTime := time.Now()

	response, err := http.DefaultClient.Post(Url, "application/json", nodeBody)
	if err != nil {
		return err
	}

	requestDuration = time.Since(reqStartTime)
	fmt.Println("Request duration", Url, requestDuration)

	copyTime := time.Now()
	_, err = io.Copy(resp, response.Body)
	if err != nil {
		return err
	}
	fmt.Println("Copy duration", "url", time.Since(copyTime))

	return nil
}

func (b *Batch) SendBatch(nodeUrl, indexerUrl string) (io.ReadCloser, error) {
	var wg sync.WaitGroup
	var err error
	errChan := make(chan error)

	// Set response into fixed order
	var (
		separateResponsesFromNode    []*bytes.Buffer
		separateResponsesFromIndexer []*bytes.Buffer
	)
	responseFromNode := new(bytes.Buffer)
	responsefromIndexer := new(bytes.Buffer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = b.SplitBatch(b.nodeReq, nodeUrl, responseFromNode)
		if err != nil {
			errChan <- err
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = b.SplitBatch(b.indexerReq, indexerUrl, responsefromIndexer)
		if err != nil {
			errChan <- err
		}
	}()
	wg.Wait()

	if len(errChan) != 0 {
		return nil, <-errChan
	}

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
	startTime := time.Now()
	defer fmt.Println("Check if req for indexer", "duration", time.Since(startTime))
	return strings.Contains(string(arg), getLogsMethod)
}

func NewBatch(a []map[string]json.RawMessage) (*Batch, error) {
	startTime := time.Now()
	defer fmt.Println("Creating batch object", "duration", time.Since(startTime))
	batch := &Batch{order: make(map[int]ReqTarget)}

	for i, obj := range a {
		if string(obj["method"]) == getLogsMethod {
			batch.indexerReq = append(batch.indexerReq, &BatchElem{request: obj, id: i})
			batch.order[i] = IndexerRequest
		} else {
			batch.nodeReq = append(batch.nodeReq, &BatchElem{request: obj, id: i})
			batch.order[i] = NodeRequest
		}
	}

	return batch, nil
}

func sendReqToIndexer(body []byte, indexerUrl string) (io.ReadCloser, error) {
	startTime := time.Now()
	defer fmt.Println("Sending common request to indexer", "duration", time.Since(startTime))
	response, err := http.DefaultClient.Post(indexerUrl, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

func main() {
	startTime := time.Now()
	defer func() {
		allDuration := time.Since(startTime)
		fmt.Println("All duration time", allDuration)
		fmt.Println("Delta", allDuration-requestDuration)
	}()
	var a []map[string]json.RawMessage
	var b map[string]json.RawMessage
	//line := []byte(`{"a": "b", "c": "d"}`)
	//line := []byte(`[{"a": "b", "c": "d"}, {"e": "b", "f": "d"}]`)
	line := []byte(`[{"method":"eth_syncing","params":[],"id":2,"jsonrpc":"2.0"},{"jsonrpc":"2.0","id":696969,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]},{"jsonrpc":"2.0","id":69696932,"method":"eth_getLogs","params":[{"toBlock":"0x7A120","fromBlock":"0x0"}]}]`)
	// line := []byte(`[{"method":"eth_syncing","params":[],"id":1,"jsonrpc":"2.0"},{"method":"eth_syncing","params":[],"id":2,"jsonrpc":"2.0"}, {"jsonrpc":"2.0","id":3,"method":"eth_getLogs","params":[{"toBlock":"0x1","fromBlock":"0x0"}]}, {"method":"eth_syncing","params":[],"id":4,"jsonrpc":"2.0"}]`)

	if ForIndexer(line) == true {
		err := json.Unmarshal(line, &a)
		if err != nil {
			err := json.Unmarshal(line, &b)
			if err != nil {
				panic(err)
			}
			_, err = sendReqToIndexer(line, indexerUrl)
			if err != nil {
				panic(err)
			}
			return
		}

		batch, err := NewBatch(a)
		if err != nil {
			panic(err)
		}

		body, err := batch.SendBatch(nodeUrl, indexerUrl)
		if err != nil {
			panic(err)
		}
		_ = body
	}
	// Batch request, needs additional processing
	// fmt.Println(a)

	// bodyBytes, err := io.ReadAll(body)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(bodyBytes))

}
