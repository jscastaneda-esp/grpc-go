package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/jscastaneda-esp/grpc/proto/testpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DoUnary(c testpb.TestServiceClient) {
	req := &testpb.GetTestRequest{
		Id: "t1",
	}

	res, err := c.GetTest(context.Background(), req)
	if err != nil {
		log.Fatal("error while calling GetTest", err)
	}

	log.Println(res)
}

func DoClientStreaming(c testpb.TestServiceClient) {
	questions := []*testpb.Question{
		{
			Id:       "q3",
			Answer:   "Azul",
			Question: "Color asociado a Golang",
			TestId:   "t1",
		},
		{
			Id:       "q4",
			Answer:   "Google",
			Question: "Quien empresa desarrollo Golang",
			TestId:   "t1",
		},
		{
			Id:       "q5",
			Answer:   "Backend",
			Question: "Especialidad de Golang",
			TestId:   "t1",
		},
	}
	stream, err := c.SetQuestions(context.Background())
	if err != nil {
		log.Fatal("error while calling SetQuestions", err)
	}
	for _, question := range questions {
		log.Println("sending question", question.GetId())
		stream.Send(question)
		time.Sleep(2 * time.Second)
	}

	msg, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal("error while receiving response", err)
	}

	log.Println("response", msg)
}

func DoServerStreaming(c testpb.TestServiceClient) {
	req := &testpb.GetStudentsPerTestRequest{
		TestId: "t1",
	}

	stream, err := c.GetStudentsPerTest(context.Background(), req)
	if err != nil {
		log.Fatal("error while calling GetStudentsPerTest", err)
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal("error while reading from stream", err)
		}

		log.Println("response", msg)
	}
}

func DoBidirectionalStreaming(c testpb.TestServiceClient) {
	answer := &testpb.TakeTestRequest{
		Answer: "42",
	}

	numberOfQuestions := 4

	waitChannel := make(chan struct{})

	stream, err := c.TakeTest(context.Background())
	if err != nil {
		log.Fatal("error while calling TakeTest", err)
	}

	go func() {
		for i := 0; i < numberOfQuestions; i++ {
			stream.Send(answer)
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				log.Fatal("error while reading from stream", err)
			}

			log.Println("response received", msg)
		}

		close(waitChannel)
	}()

	<-waitChannel
}

func main() {
	cc, err := grpc.Dial("localhost:5061", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("could not connect:", err)
	}
	defer cc.Close()

	c := testpb.NewTestServiceClient(cc)
	// DoUnary(c)
	// DoClientStreaming(c)
	// DoServerStreaming(c)
	DoBidirectionalStreaming(c)
}
