package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var CertDir = os.Getenv("CREDS_DIR")

func TestMonitor(t *testing.T) {
	if len(CertDir) <= 0 {
		fmt.Println("Please Set CREDS_DIR")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mt := Multitasking.NewMultitasking("TestPool", nil)
	mt.Register(GenNumbers,
		func(ec Multitasking.ExecuteController, a any) any {
			task := a.(Task)
			t := time.Duration(task.A%2) * time.Duration(time.Second)
			//fmt.Printf("> Sleep %s\n",t)
			time.Sleep(t)
			return task.A + task.B
		})

	go func() {
		ms, err := Multitasking.NewMonitorServer(mt)
		if err != nil {
			fmt.Printf("Monitor Server Error: %s\n", err)
			return
		}

		cert, err := credentials.NewServerTLSFromFile(path.Join(CertDir, "server.crt"), path.Join(CertDir, "server.key"))
		if err != nil {
			log.Fatalf("failed to load cert: %v", err)
		}

		err = Multitasking.StartMonitoringServer("0.0.0.0:50051", ms, grpc.Creds(cert))
		if err != nil {
			fmt.Printf("Starting Monitor Server Error: %s\n", err)
			return
		}
	}()

	r, err := mt.Run(ctx, 20)
	if err != nil {
		fmt.Printf("Pool Run Error: %s\n", err)
		return
	}

	fmt.Printf("Total: %d\n", len(r))

	// creds, err := credentials.NewClientTLSFromFile(path.Join(CertDir, "server.crt"), "localhost")
	// if err != nil {
	// 	log.Fatalf("could not load tls cert: %s", err)
	// }

	// client, err := Multitasking.NewMonitorClient("127.0.0.1:50051", grpc.WithTransportCredentials(creds))
	// if err != nil {
	// 	fmt.Printf("Monitor Client Error: %s\n", err)
	// 	return
	// }

	// fmt.Println("[Start Stream Metrics]")
	// stream, err := client.StreamMetrics(ctx, 2*time.Second)
	// if err != nil {
	// 	fmt.Printf("Monitor Client Stream Error: %s\n", err)
	// 	return
	// }

	// for {
	// 	//fmt.Println("Reading...")
	// 	m, err := stream.Receive()
	// 	if err != nil {
	// 		if status.Code(err) != codes.Canceled {
	// 			fmt.Printf("Metrics Error: %s\n", err)
	// 		} else {
	// 			fmt.Printf("[Metrics Stream Closed]\n")
	// 		}
	// 		return
	// 	}
	// 	fmt.Printf("> Metrics %s\n", m)
	// }
}
