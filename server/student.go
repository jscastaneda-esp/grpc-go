package server

import (
	"context"

	"github.com/jscastaneda-esp/grpc/models"
	"github.com/jscastaneda-esp/grpc/proto/studentpb"
	"github.com/jscastaneda-esp/grpc/repository"
)

type StudentServer struct {
	studentpb.UnimplementedStudentServiceServer
	repo repository.Repository
}

func (s *StudentServer) GetStudent(ctx context.Context, req *studentpb.GetStudentRequest) (*studentpb.Student, error) {
	student, err := s.repo.GetStudent(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	return &studentpb.Student{
		Id:   student.Id,
		Name: student.Name,
		Age:  student.Age,
	}, nil
}

func (s *StudentServer) SetStudent(ctx context.Context, req *studentpb.Student) (*studentpb.SetStudentResponse, error) {
	student := &models.Student{
		Id:   req.GetId(),
		Name: req.GetName(),
		Age:  req.GetAge(),
	}
	err := s.repo.SetStudent(ctx, student)
	if err != nil {
		return nil, err
	}

	return &studentpb.SetStudentResponse{
		Id: student.Id,
	}, nil
}

func NewStudentServer(repo repository.Repository) *StudentServer {
	return &StudentServer{repo: repo}
}
