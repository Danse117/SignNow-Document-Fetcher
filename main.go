package main

import (
	// Standard imports
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"io"
	"net/http"
	"os"

	// AWS imports
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// AWS Functions
func lambdaHandler(event Request) (string, error) {
	err := getDocumentsInTimeFrame(event.StartDate, event.EndDate)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Documents between %s and %s processed successfully.", event.StartDate, event.EndDate), nil
}

func uploadToS3(bucketName, key string, content []byte) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	}))
	svc := s3.New(sess)

	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	return err
}

// Structs for the full JSON response from the SignNow API
type Document struct {
	DocumentID   string      `json:"document_id"`
	UserID       string      `json:"user_id"`
	DocumentName string      `json:"document_name"`
	Created      string      `json:"created"`
	Updated      string      `json:"updated"`
	Signatures   []Signature `json:"signatures"`
	Fields       []Field     `json:"fields"`
	Texts        []Text      `json:"texts"`
}

type Signature struct {
	Id        string `json:"id"`
	UserId    string `json:"user_id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Data      string `json:"data"`
}

type Field struct {
	Id         string `json:"id"`
	Type       string `json:"type"`
	RoleId     string `json:"role_id"`
	Originator string `json:"originator"`
	Fulfiller  string `json:"fulfiller"`
	Value      string `json:"value"`
}

type Text struct {
	Id         string `json:"id"`
	UserId     string `json:"user_id"`
	PageNumber string `json:"page_number"`
	Email      string `json:"email"`
	Size       string `json:"size"`
	Data       string `json:"data"`
	Created    string `json:"created"`
}

type Request struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
}

// Gets signed documents within a specific time frame
func getDocumentsInTimeFrame(startDate, endDate string) error {

	// Init variables
	accessToken := os.Getenv("API_ACCESS_TOKEN")
	if accessToken == "" {
		return errors.New("*** Missing API_ACCESS_TOKEN in environment variables ***")
	}
	url := fmt.Sprintf("https://api.signnow.com/user/documentsv2?filter[updated][gte]=%s&filter[updated][lte]=%s", startDate, endDate)

	// Create HTTP request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return errors.New("*** Error creating request ***: " + err.Error())
	}

	// Set headers for SignNow API
	req.Header.Add("Authorization", "Bearer "+accessToken)
	req.Header.Add("Accept", "application/json")

	// Send request to SignNow API
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.New("*** Error making request ***: " + err.Error())
	}
	defer res.Body.Close()

	// Read response body (what was returned from API)
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return errors.New("*** Error reading response ***: " + err.Error())
	}

	// Parse JSON response using the Document struct to filter out
	// sections of the body that are not needed
	var documents []Document
	if err := json.Unmarshal(body, &documents); err != nil {
		return errors.New("*** Error parsing JSON ***: " + err.Error())
	}

	// Structure JSON
	completeJSON, err := json.MarshalIndent(documents, "", "  ")
	if err != nil {
		return errors.New("*** Error marshalling JSON ***: " + err.Error())
	}

	// Upload JSON fields to S3
	s3Key := fmt.Sprintf("signed_documents_%s_to_%s.json", startDate, endDate)
	bucketName := os.Getenv("S3_BUCKET_NAME") // Set this in Lambda environment vars

	err = uploadToS3(bucketName, s3Key, completeJSON)
	if err != nil {
		return errors.New("*** Error uploading to S3 ***: " + err.Error())
	}

	fmt.Printf("Signed documents from %s to %s uploaded to s3://%s/%s\n", startDate, endDate, bucketName, s3Key)
	fmt.Printf("Found %d signed documents in the specified time frame\n", len(documents))
	return nil
}

// Main function
func main() {
	lambda.Start(lambdaHandler)
	/*
		Used for when testing locally
		// Format as YYYY-MM-DD
		if err := getDocumentsInTimeFrame("2024-01-01", "2025-06-15"); err != nil {
			log.Fatal("***Error getting documents from within time frame***:", err)
		}
	*/
}
