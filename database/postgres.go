package database

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"github.com/jscastaneda-esp/grpc/models"
	_ "github.com/lib/pq"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(url string) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	return &PostgresRepository{db: db}, nil
}

func (repo *PostgresRepository) GetStudent(ctx context.Context, id string) (*models.Student, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT * FROM students WHERE id = $1 LIMIT 1", id)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	if rows.Next() {
		student := new(models.Student)
		err := rows.Scan(&student.Id, &student.Name, &student.Age)
		if err != nil {
			return nil, err
		}

		return student, nil
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nil, errors.New("student not found")
}

func (repo *PostgresRepository) SetStudent(ctx context.Context, student *models.Student) error {
	result, err := repo.db.ExecContext(ctx, "INSERT INTO students (id, name, age) VALUES ($1, $2, $3)", student.Id, student.Name, student.Age)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return errors.New("not insert student")
	}

	return nil
}

func (repo *PostgresRepository) GetTest(ctx context.Context, id string) (*models.Test, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT * FROM tests WHERE id = $1 LIMIT 1", id)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	if rows.Next() {
		test := new(models.Test)
		err := rows.Scan(&test.Id, &test.Name)
		if err != nil {
			return nil, err
		}

		return test, nil
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nil, errors.New("test not found")
}

func (repo *PostgresRepository) SetTest(ctx context.Context, student *models.Test) error {
	result, err := repo.db.ExecContext(ctx, "INSERT INTO tests (id, name) VALUES ($1, $2)", student.Id, student.Name)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return errors.New("not insert test")
	}

	return nil
}

func (repo *PostgresRepository) SetQuestion(ctx context.Context, question *models.Question) error {
	result, err := repo.db.ExecContext(ctx, "INSERT INTO questions (id, test_id, question, answer) VALUES ($1, $2, $3, $4)", question.Id, question.TestId, question.Question, question.Answer)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return errors.New("not insert question")
	}

	return nil
}

func (repo *PostgresRepository) SetEnrollment(ctx context.Context, enrollment *models.Enrollment) error {
	result, err := repo.db.ExecContext(ctx, "INSERT INTO enrollments (student_id, test_id) VALUES ($1, $2)", enrollment.StudentId, enrollment.TestId)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return errors.New("not insert question")
	}

	return nil
}

func (repo *PostgresRepository) GetStudentsPerTest(ctx context.Context, testId string) ([]*models.Student, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, name, age FROM students WHERE id IN (SELECT student_id FROM enrollments WHERE test_id = $1)", testId)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	var students []*models.Student
	for rows.Next() {
		student := new(models.Student)
		if err = rows.Scan(&student.Id, &student.Name, &student.Age); err == nil {
			students = append(students, student)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return students, nil
}

func (repo *PostgresRepository) GetQuestionsPerTest(ctx context.Context, testId string) ([]*models.Question, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, test_id, question, answer FROM questions WHERE test_id = $1", testId)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	var questions []*models.Question
	for rows.Next() {
		question := new(models.Question)
		if err = rows.Scan(&question.Id, &question.TestId, &question.Question, &question.Answer); err == nil {
			questions = append(questions, question)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return questions, nil
}
