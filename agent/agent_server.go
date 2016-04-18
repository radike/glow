// Package agent runs on servers with computing resources, and executes
// tasks sent by driver.
package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/glow/driver/cmd"
	"github.com/chrislusf/glow/netchan"
	"github.com/chrislusf/glow/resource"
	"github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/glow/util"
	"github.com/golang/protobuf/proto"
)

type AgentServerOption struct {
	Master            *string
	Host              *string
	Port              *int
	Dir               *string
	DataCenter        *string
	Rack              *string
	MaxExecutor       *int
	MemoryMB          *int64
	CPULevel          *int
	CleanRestart      *bool
	CertFiles         netchan.CertFiles
	ProvidedResources *string
}

type AgentServer struct {
	Option                *AgentServerOption
	Master                string
	wg                    sync.WaitGroup
	listener              net.Listener
	computeResource       *resource.ComputeResource
	allocatedResource     *resource.ComputeResource
	allocatedResourceLock sync.Mutex
	storageBackend        *LocalDatasetShardsManager
	localExecutorManager  *LocalExecutorManager
}

func NewAgentServer(option *AgentServerOption) *AgentServer {
	absoluteDir, err := filepath.Abs(util.CleanPath(*option.Dir))
	if err != nil {
		panic(err)
	}
	println("starting in", absoluteDir)
	option.Dir = &absoluteDir

	as := &AgentServer{
		Option:         option,
		Master:         *option.Master,
		storageBackend: NewLocalDatasetShardsManager(*option.Dir, *option.Port),
		computeResource: &resource.ComputeResource{
			CPUCount: *option.MaxExecutor,
			CPULevel: *option.CPULevel,
			MemoryMB: *option.MemoryMB,
		},
		allocatedResource:    &resource.ComputeResource{},
		localExecutorManager: newLocalExecutorsManager(),
	}

	err = as.init()
	if err != nil {
		panic(err)
	}

	return as
}

func (r *AgentServer) init() (err error) {
	tlsConfig := r.Option.CertFiles.MakeTLSConfig()
	if tlsConfig == nil {
		r.listener, err = net.Listen("tcp", *r.Option.Host+":"+strconv.Itoa(*r.Option.Port))
	} else {
		r.listener, err = tls.Listen("tcp", *r.Option.Host+":"+strconv.Itoa(*r.Option.Port), tlsConfig)
	}
	util.SetupHttpClient(tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("AgentServer starts on", *r.Option.Host+":"+strconv.Itoa(*r.Option.Port))

	if *r.Option.CleanRestart {
		if fileInfos, err := ioutil.ReadDir(r.storageBackend.dir); err == nil {
			suffix := fmt.Sprintf("-%d.dat", *r.Option.Port)
			for _, fi := range fileInfos {
				name := fi.Name()
				if !fi.IsDir() && strings.HasSuffix(name, suffix) {
					// println("removing old dat file:", name)
					os.Remove(filepath.Join(r.storageBackend.dir, name))
				}
			}
		}
	}

	return
}

func (as *AgentServer) Run() {
	//register agent
	killHeartBeaterChan := make(chan bool, 1)
	go client.NewHeartBeater(*as.Option.Host, *as.Option.Port, as.Master).StartAgentHeartBeat(killHeartBeaterChan, func(values url.Values) {
		resource.AddToValues(values, as.computeResource, as.allocatedResource)
		values.Add("dataCenter", *as.Option.DataCenter)
		values.Add("rack", *as.Option.Rack)
		values.Add("resources", *as.Option.ProvidedResources)
	})

	for {
		// Listen for an incoming connection.
		conn, err := as.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		as.wg.Add(1)
		go func() {
			defer as.wg.Done()
			defer conn.Close()
			as.handleRequest(conn)
		}()
	}
}

func (r *AgentServer) Stop() {
	r.listener.Close()
	r.wg.Wait()
}

// Handles incoming requests.
func (r *AgentServer) handleRequest(conn net.Conn) {

	buf := make([]byte, 4)

	f, message, err := util.ReadBytes(conn, buf)

	tlscon, ok := conn.(*tls.Conn)
	if ok {
		state := tlscon.ConnectionState()
		if !state.HandshakeComplete {
			log.Printf("Failed to tls handshake with: %+v", tlscon.RemoteAddr())
			return
		}
	}

	if f != util.Data {
		//strange if this happens
		println("read", len(message.Bytes()), "request flag:", f, "data", string(message.Data()))
		return
	}

	if err != nil {
		log.Printf("Failed to read command %s:%v", string(message.Data()), err)
	}
	if bytes.HasPrefix(message.Data(), []byte("PUT ")) {
		name := string(message.Data()[4:])
		r.handleLocalWriteConnection(conn, name)
	} else if bytes.HasPrefix(message.Data(), []byte("GET ")) {
		name := string(message.Data()[4:])
		offset := util.ReadUint64(conn)
		r.handleReadConnection(conn, name, int64(offset))
	} else if bytes.HasPrefix(message.Data(), []byte("CMD ")) {
		newCmd := &cmd.ControlMessage{}
		err := proto.Unmarshal(message.Data()[4:], newCmd)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
		}
		reply := r.handleCommandConnection(conn, newCmd)
		if reply != nil {
			data, err := proto.Marshal(reply)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}
			conn.Write(data)
		}
	}

}
