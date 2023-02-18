package main

import (
	"log"
	"net"

	"github.com/jscastaneda-esp/grpc/database"
	"github.com/jscastaneda-esp/grpc/proto/studentpb"
	"github.com/jscastaneda-esp/grpc/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	listener, err := net.Listen("tcp", ":5060")
	if err != nil {
		log.Fatal(err)
	}

	repo, err := database.NewPostgresRepository("postgres://postgres:postgres@192.168.1.14:54321/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	server := server.NewStudentServer(repo)

	s := grpc.NewServer()
	studentpb.RegisterStudentServiceServer(s, server)
	reflection.Register(s)

	if err := s.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
