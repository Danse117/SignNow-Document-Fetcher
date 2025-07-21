/*
 Program is called when user.document.complete event is triggered

 Gets payload from webhook and parses it to get document_ID and subscription_ID

 Uses document_ID in SignNow API (https://api.signnow.com/document/{document_id}) to reterive doucment
 JSON, response is parsed and only specific fields are saved

After document is saved, a webhook event is called for the subscription_ID
which deletes the webhook for user.document.complete
*/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type LambdaEvent struct {
	Body                  string                 `json:"body"`                  // The request body as a string
	Headers               map[string]string      `json:"headers"`               // Request headers
	HTTPMethod            string                 `json:"httpMethod"`            // HTTP method (GET, POST, etc.)
	Path                  string                 `json:"path"`                  // Request path
	PathParameters        map[string]string      `json:"pathParameters"`        // Path parameters
	QueryStringParameters map[string]string      `json:"queryStringParameters"` // Query parameters
	RequestContext        map[string]interface{} `json:"requestContext"`        // Request context
	IsBase64Encoded       bool                   `json:"isBase64Encoded"`       // Whether body is base64 encoded
}

type LambdaResponse struct {
	StatusCode int               `json:"statusCode"` // HTTP status codes
	Headers    map[string]string `json:"headers"`    // HTTP response headers
	Body       string            `json:"body"`       // Response body as JSON string
}

// Structs for the full JSON response from the SignNow API

// Temporary struct for parsing the original JSON that includes Fields
type DocumentWithFields struct {
	DocumentID      string        `json:"id"`
	UserID          string        `json:"user_id"`
	OwnerID         string        `json:"owner"`
	OwnerName       string        `json:"owner_name"`
	DocumentName    string        `json:"document_name"`
	VersionTime     string        `json:"version_time"`
	Created         string        `json:"created"`
	Updated         string        `json:"updated"`
	Signatures      []Signature   `json:"signatures"`
	Fields          []Field       `json:"fields"`
	Texts           []Text        `json:"texts"`
	RadioButtons    []RadioButton `json:"radiobuttons"`
	Checks          []Check       `json:"checks"`
	Seals           []Seal        `json:"seals"`
	SignerFirstName string        `json:"signer_first_name"`
	SignerLastName  string        `json:"signer_last_name"`
}

type Document struct {
	DocumentID      string        `json:"id"`
	UserID          string        `json:"user_id"`
	OwnerID         string        `json:"owner"`
	OwnerName       string        `json:"owner_name"`
	DocumentName    string        `json:"document_name"`
	VersionTime     string        `json:"version_time"`
	Created         string        `json:"created"`
	Updated         string        `json:"updated"`
	Signatures      []Signature   `json:"signatures"`
	Texts           []Text        `json:"texts"`
	RadioButtons    []RadioButton `json:"radiobuttons"`
	Checks          []Check       `json:"checks"`
	Seals           []Seal        `json:"seals"`
	SignerFirstName string        `json:"signer_first_name"`
	SignerLastName  string        `json:"signer_last_name"`
}

type Signature struct {
	Id         string `json:"id"`
	UserId     string `json:"user_id"`
	Email      string `json:"email"`
	Data       string `json:"data"`
	Subtype    string `json:"subtype"`
	FieldName  string `json:"field_name,omitempty"`
	FieldLabel string `json:"field_label,omitempty"`
}

type Field struct {
	Id             string                 `json:"id"`
	Type           string                 `json:"type"`
	RoleId         string                 `json:"role_id"`
	Originator     string                 `json:"originator"`
	Fulfiller      string                 `json:"fulfiller"`
	JSONAttributes map[string]interface{} `json:"json_attributes"`
	FieldID        string                 `json:"field_id"`
	ElementID      string                 `json:"element_id"`
}

type Text struct {
	Id         string `json:"id"`
	UserId     string `json:"user_id"`
	PageNumber string `json:"page_number"`
	Email      string `json:"email"`
	Size       string `json:"size"`
	Data       string `json:"data"`
	Created    string `json:"created"`
	Name       string `json:"name"`
	Label      string `json:"label"`
}

type RadioButtonOption struct {
	RadioID string `json:"radio_id"`
	Created string `json:"created"`
	Checked string `json:"checked"`
	Value   string `json:"value"`
}

type RadioButton struct {
	Id                     string              `json:"id"`
	UserId                 string              `json:"user_id"`
	Name                   string              `json:"name"`
	Status                 string              `json:"status"`
	ServerCreatedTimestamp string              `json:"server_created_timestamp"`
	PageNumber             string              `json:"page_number"`
	Radio                  []RadioButtonOption `json:"radio"`
	FieldName              string              `json:"field_name,omitempty"`
	FieldLabel             string              `json:"field_label,omitempty"`
}

type Check struct {
	Id         string `json:"id"`
	UserId     string `json:"user_id"`
	PageNumber string `json:"page_number"`
	Email      string `json:"email"`
	Created    string `json:"created"`
	FieldName  string `json:"field_name,omitempty"`
	FieldLabel string `json:"field_label,omitempty"`
}

type Seal struct {
	Id             string                 `json:"id"`
	Type           string                 `json:"type"`
	RoleId         string                 `json:"role_id"`
	Originator     string                 `json:"originator"`
	Fulfiller      string                 `json:"fulfiller"`
	JSONAttributes map[string]interface{} `json:"json_attributes"`
	FieldID        string                 `json:"field_id"`
	ElementID      string                 `json:"element_id"`
}

// handleRequest is the main Lambda function handler that processes document parsing requests
// Extracts the document ID from the request and retrieves the full document from SignNow
func handleRequest(ctx context.Context, event interface{}) (LambdaResponse, error) {
	log.Println("=== Document Parser Lambda function triggered ===")

	eventJSON, _ := json.Marshal(event)
	log.Printf("Raw event received: %s", string(eventJSON))

	// Check if payload has documentId
	var directPayload map[string]interface{}
	err := json.Unmarshal(eventJSON, &directPayload)
	if err == nil {
		// Check if it has documentId
		if _, hasDocumentId := directPayload["documentId"]; hasDocumentId {
			log.Printf("Using direct payload mode - detected documentId")
			return processDocumentRequest(directPayload)
		}
	}

	var apiGatewayEvent LambdaEvent
	err = json.Unmarshal(eventJSON, &apiGatewayEvent)
	if err != nil {
		log.Printf("Error parsing as API Gateway event: %v", err)
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Invalid event format: %v"}`, err),
		}, nil
	}

	// Use API Gateway event
	log.Printf("Using API Gateway event mode")
	log.Printf("HTTP Method: %s", apiGatewayEvent.HTTPMethod)
	log.Printf("Path: %s", apiGatewayEvent.Path)
	log.Printf("Body length: %d", len(apiGatewayEvent.Body))
	log.Printf("Body: '%s'", apiGatewayEvent.Body)

	// Check if body is empty
	if apiGatewayEvent.Body == "" {
		log.Printf("Error: Empty request body")
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       `{"error": "Empty request body"}`,
		}, nil
	}

	// Parse payload
	var requestPayload map[string]interface{}
	err = json.Unmarshal([]byte(apiGatewayEvent.Body), &requestPayload)
	if err != nil {
		log.Printf("Error parsing request payload: %v", err)
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Invalid JSON payload: %v"}`, err),
		}, nil
	}

	return processDocumentRequest(requestPayload)
}

// processDocumentRequest processes the document request and retrieves the document from SignNow
func processDocumentRequest(requestPayload map[string]interface{}) (LambdaResponse, error) {
	// Extract document ID from the request
	var documentID string
	var found bool

	if documentID, found = requestPayload["documentId"].(string); found {
		log.Printf("Found documentId: %s", documentID)
	} else if documentID, found = requestPayload["document_id"].(string); found {
		log.Printf("Found document_id: %s", documentID)
	} else if content, ok := requestPayload["content"].(map[string]interface{}); ok {
		// Try inside the content object
		if documentID, found = content["documentId"].(string); found {
			log.Printf("Found documentId in content: %s", documentID)
		} else if documentID, found = content["document_id"].(string); found {
			log.Printf("Found document_id in content: %s", documentID)
		}
	}

	if documentID == "" {
		log.Printf("Error: Document ID not found in request or content")
		log.Printf("Available keys: %v", getKeys(requestPayload))
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Document ID not found in request or content", "available_keys": %v}`, getKeys(requestPayload)),
		}, nil
	}

	log.Printf("Processing document ID: %s", documentID)

	// Get the document details from the SignNow API
	documentDetails, err := getDocumentDetails(documentID)
	if err != nil {
		log.Printf("Error getting document details: %v", err)
		return LambdaResponse{
			StatusCode: 500,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Failed to get document details: %v"}`, err),
		}, nil
	}

	// Parse the document details to get the specific fields
	parsedDocument := documentDetails

	// Save the document to S3
	s3URL, err := saveDocumentToS3(parsedDocument, documentID)
	if err != nil {
		log.Printf("Error saving document to S3: %v", err)
		return LambdaResponse{
			StatusCode: 500,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Failed to save document to S3: %v"}`, err),
		}, nil
	}

	log.Printf("Document processed successfully: %s", documentID)
	log.Printf("Document saved to S3: %s", s3URL)

	// Return success response with the parsed document and S3 URL
	response := map[string]interface{}{
		"message":       "Document processed and saved successfully",
		"document_id":   documentID,
		"document_data": parsedDocument,
		"s3_url":        s3URL,
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
	}

	// Marshal response to JSON
	responseJSON, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		return LambdaResponse{
			StatusCode: 500,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Failed to marshal response: %v"}`, err),
		}, nil
	}

	return LambdaResponse{
		StatusCode: 200,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(responseJSON),
	}, nil
}

// getKeys: extract keys from a map for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func main() {
	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		lambda.Start(handleRequest)
	} else {
		log.Println("Running locally - testing with hardcoded document ID")
		documentID := "c8ad674ee71b4c0388ebb9eb6af58a1faa561128"
		_, err := getDocumentDetails(documentID)
		if err != nil {
			log.Printf("Error getting document details: %v", err)
			return
		}
	}
}

func getDocumentDetails(documentID string) (*Document, error) {
	// Get the SignNow API token from environment variable
	apiToken := os.Getenv("SIGNNOW_API_TOKEN")
	if apiToken == "" {
		// Fallback to hardcoded token for local testing
		apiToken = "54dabbcbcd73c76cb77d503c860d4d2cb588ada56357462ff1ae36c589421518"
	}

	url := fmt.Sprintf("https://api.signnow.com/document/%s", documentID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Add("Authorization", "Bearer "+apiToken)
	req.Header.Add("Accept", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	// Check response status
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", res.StatusCode, string(body))
	}

	// Parse JSON response with fields included
	var documentWithFields DocumentWithFields
	err = json.Unmarshal(body, &documentWithFields)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON response: %v", err)
	}

	// Convert to final Document struct and enhance texts, radiobuttons, checks, and signatures with name/label from fields
	document := &Document{
		DocumentID:      documentWithFields.DocumentID,
		UserID:          documentWithFields.UserID,
		OwnerID:         documentWithFields.OwnerID,
		OwnerName:       documentWithFields.OwnerName,
		DocumentName:    documentWithFields.DocumentName,
		VersionTime:     documentWithFields.VersionTime,
		Created:         documentWithFields.Created,
		Updated:         documentWithFields.Updated,
		Signatures:      signaturesWithFieldData(documentWithFields.Signatures, documentWithFields.Fields),
		Texts:           textsWithFieldData(documentWithFields.Texts, documentWithFields.Fields),
		RadioButtons:    radioButtonsWithFieldData(documentWithFields.RadioButtons, documentWithFields.Fields),
		Checks:          checksWithFieldData(documentWithFields.Checks, documentWithFields.Fields),
		Seals:           documentWithFields.Seals,
		SignerFirstName: documentWithFields.SignerFirstName,
		SignerLastName:  documentWithFields.SignerLastName,
	}

	return document, nil
}

/*
The following functions are needed as per the response, the individual fields
do not include the name and label fields. They must be extracted from the fields
array and appeneded to the individual fields.
*/
func textsWithFieldData(texts []Text, fields []Field) []Text {
	// Create a map of field element_id to field for quick lookup
	fieldMap := make(map[string]*Field)
	for i := range fields {
		fieldMap[fields[i].ElementID] = &fields[i]
	}

	enhancedTexts := make([]Text, len(texts))
	for i, text := range texts {
		enhancedTexts[i] = text

		if field, exists := fieldMap[text.Id]; exists {
			if name, ok := field.JSONAttributes["name"].(string); ok {
				enhancedTexts[i].Name = name
			}
			if label, ok := field.JSONAttributes["label"].(string); ok {
				enhancedTexts[i].Label = label
			}
		}
	}

	return enhancedTexts
}

func radioButtonsWithFieldData(radiobuttons []RadioButton, fields []Field) []RadioButton {
	// Create a map of field element_id to field for quick lookup
	fieldMap := make(map[string]*Field)
	for i := range fields {
		fieldMap[fields[i].ElementID] = &fields[i]
	}

	enhancedRadioButtons := make([]RadioButton, len(radiobuttons))
	for i, radiobutton := range radiobuttons {
		enhancedRadioButtons[i] = radiobutton

		if field, exists := fieldMap[radiobutton.Id]; exists {
			if name, ok := field.JSONAttributes["name"].(string); ok {
				enhancedRadioButtons[i].FieldName = name
			}
			if label, ok := field.JSONAttributes["label"].(string); ok {
				enhancedRadioButtons[i].FieldLabel = label
			}
		}
	}

	return enhancedRadioButtons
}

func checksWithFieldData(checks []Check, fields []Field) []Check {
	fieldMap := make(map[string]*Field)
	for i := range fields {
		fieldMap[fields[i].ElementID] = &fields[i]
	}

	enhancedChecks := make([]Check, len(checks))
	for i, check := range checks {
		enhancedChecks[i] = check

		if field, exists := fieldMap[check.Id]; exists {
			if name, ok := field.JSONAttributes["name"].(string); ok {
				enhancedChecks[i].FieldName = name
			}
			if label, ok := field.JSONAttributes["label"].(string); ok {
				enhancedChecks[i].FieldLabel = label
			}
		}
	}

	return enhancedChecks
}
func signaturesWithFieldData(signatures []Signature, fields []Field) []Signature {
	fieldMap := make(map[string]*Field)
	for i := range fields {
		fieldMap[fields[i].ElementID] = &fields[i]
	}

	enhancedSignatures := make([]Signature, len(signatures))
	for i, signature := range signatures {
		enhancedSignatures[i] = signature

		if field, exists := fieldMap[signature.Id]; exists {
			if name, ok := field.JSONAttributes["name"].(string); ok {
				enhancedSignatures[i].FieldName = name
			}
			if label, ok := field.JSONAttributes["label"].(string); ok {
				enhancedSignatures[i].FieldLabel = label
			}
		}
	}

	return enhancedSignatures
}

// saveDocumentToS3 saves the parsed document to AWS S3
// Returns the S3 URL of the saved document
func saveDocumentToS3(document *Document, documentID string) (string, error) {
	// Get S3 bucket name from environment variable
	bucketName := os.Getenv("S3_BUCKET_NAME")
	if bucketName == "" {
		return "", fmt.Errorf("S3_BUCKET_NAME environment variable is required")
	}

	// Get AWS region from environment variable
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-2"
	}

	// Create AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return "", fmt.Errorf("error creating AWS session: %v", err)
	}

	// Create S3 service client
	s3Client := s3.New(sess)

	// Convert document to JSON
	jsonData, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error marshaling document to JSON: %v", err)
	}

	// Create filename with firstName-lastName-signed-document_name format
	firstName := document.SignerFirstName
	lastName := document.SignerLastName
	documentName := document.DocumentName

	if len(documentName) > 0 {
		// Remove file extension
		for i := len(documentName) - 1; i >= 0; i-- {
			if documentName[i] == '.' {
				documentName = documentName[:i]
				break
			}
		}
		// Replace spaces and special characters with hyphens
		documentName = strings.ReplaceAll(documentName, " ", "-")
		documentName = strings.ReplaceAll(documentName, "_", "-")
	}

	// If no names available, use document ID as fallback
	if firstName == "" && lastName == "" {
		firstName = "Unknown"
		lastName = "Signer"
	}

	// Create S3 key with timestamp for uniqueness
	timestamp := time.Now().UTC().Format("2006-01-02-15-04-05")
	s3Key := fmt.Sprintf("documents/%s/%s-%s-signed-%s-%s.json",
		time.Now().UTC().Format("2006-01-02"), // Date folder
		firstName,
		lastName,
		documentName,
		timestamp)

	// Upload to S3
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(s3Key),
		Body:        strings.NewReader(string(jsonData)),
		ContentType: aws.String("application/json"),
		Metadata: map[string]*string{
			"document-id":      aws.String(documentID),
			"document-name":    aws.String(document.DocumentName),
			"signer-first":     aws.String(firstName),
			"signer-last":      aws.String(lastName),
			"upload-timestamp": aws.String(timestamp),
		},
	})
	if err != nil {
		return "", fmt.Errorf("error uploading to S3: %v", err)
	}
	s3URL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucketName, region, s3Key)

	log.Printf("Document saved to S3: %s", s3URL)
	return s3URL, nil
}
