package Multitasking

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/B9O2/Multitasking/monitor"
	"google.golang.org/grpc"
)

const TimeLayout = "2006-01-02 15:04:05"

type MonitorServer struct {
	monitor.UnimplementedMonitorServiceServer
	mt        *Multitasking
	logReader func(theadID int64, skipLine uint64, after time.Time) []string
}

func (ms *MonitorServer) SetLogReader(logReader func(theadID int64, skipLine uint64, after time.Time) []string) {
	ms.logReader = logReader
}

func (ms *MonitorServer) StreamEvents(req *monitor.StreamEventsRequest, stream grpc.ServerStreamingServer[monitor.Events]) error {
	//fmt.Println("Server stream metrics starting")
	interval := time.Duration(req.Interval)
	startTime := time.Now()
	skipLines := uint64(0)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			//fmt.Println("Server metrics sending")
			var logs []string
			if ms.logReader == nil {
				logs = []string{fmt.Sprintf("[%s]Monitor server has no log reader. Skip Lines: %d Start Time: %s", time.Now().Format(TimeLayout), skipLines, startTime.Format(TimeLayout))}
			} else {
				logs = ms.logReader(req.ThreadId, skipLines, startTime)
			}

			events := &monitor.Events{
				Logs: logs,
			}
			skipLines += uint64(len(logs))

			if err := stream.Send(events); err != nil {
				return err
			}
			//fmt.Println("Server metrics sent")
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (ms *MonitorServer) StreamStatus(req *monitor.StreamStatusRequest, stream grpc.ServerStreamingServer[monitor.Status]) error {
	//fmt.Println("Server stream metrics starting")
	interval := time.Duration(req.Interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			//fmt.Println("Server metrics sending")

			status := &monitor.Status{
				Name:        ms.mt.Name(),
				TotalTask:   ms.mt.TotalTask(),
				TotalRetry:  ms.mt.TotalRetry(),
				TotalResult: ms.mt.TotalResult(),
				RetrySize:   ms.mt.MaxRetryQueue(),
				ThreadsDetail: &monitor.ThreadsDetail{
					ThreadsStatus: ms.mt.threadsDetail.AllStatus(),
					ThreadsCount:  ms.mt.threadsDetail.AllCounter(),
				},
			}

			if err := stream.Send(status); err != nil {
				return err
			}
			//fmt.Println("Server metrics sent")
		case <-stream.Context().Done():
			return nil
		}
	}
}

func NewMonitorServer(mt *Multitasking) (*MonitorServer, error) {
	ms := &MonitorServer{
		mt: mt,
	}
	return ms, nil
}

func StartMonitoringServer(address string, ms *MonitorServer, opts ...grpc.ServerOption) error {
	server := grpc.NewServer(opts...)
	monitor.RegisterMonitorServiceServer(server, ms)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	return server.Serve(lis)
}

type StatusStream struct {
	stream grpc.ServerStreamingClient[monitor.Status]
}

func (ms *StatusStream) Receive() (*monitor.Status, error) {
	return ms.stream.Recv()
}

type EventsStream struct {
	stream grpc.ServerStreamingClient[monitor.Events]
}

func (es *EventsStream) Receive() (*monitor.Events, error) {
	return es.stream.Recv()
}

type MonitorClient struct {
	conn *grpc.ClientConn
	msc  monitor.MonitorServiceClient
}

func (mc *MonitorClient) StreamStatus(ctx context.Context, interval time.Duration) (*StatusStream, error) {
	stream, err := mc.msc.StreamStatus(ctx, &monitor.StreamStatusRequest{
		Interval: uint64(interval),
	})
	if err != nil {
		return nil, err
	}
	return &StatusStream{
		stream: stream,
	}, nil

}

// StreamEvents threadID为负数代表所有日志
func (mc *MonitorClient) StreamEvents(ctx context.Context, interval time.Duration, threadID int64) (*EventsStream, error) {
	stream, err := mc.msc.StreamEvents(ctx, &monitor.StreamEventsRequest{
		Interval: uint64(interval),
		ThreadId: threadID,
	})
	if err != nil {
		return nil, err
	}
	return &EventsStream{
		stream: stream,
	}, nil

}

func (mc *MonitorClient) Close() error {
	return mc.conn.Close()
}

func NewMonitorClient(addr string, opts ...grpc.DialOption) (*MonitorClient, error) {
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	msc := monitor.NewMonitorServiceClient(conn)

	mc := &MonitorClient{
		conn: conn,
		msc:  msc,
	}

	return mc, nil
}
