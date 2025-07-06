/*
Creates a webhook event for a specific document_ID after the document is completed.

document_ID: Obtained from webhook when user.document.create event is triggered
*/

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
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

// LambdaResponse represents the response from the Lambda function
type LambdaResponse struct {
	StatusCode int               `json:"statusCode"` // HTTP status codes
	Headers    map[string]string `json:"headers"`    // HTTP response headers
	Body       string            `json:"body"`       // Response body as JSON string
}

// handleRequest is the main Lambda function handler that processes incoming webhook events
// It extracts the document ID from the webhook payload and creates a document.complete webhook
func handleRequest(ctx context.Context, event interface{}) (LambdaResponse, error) {
	log.Println("=== Lambda function triggered ===")

	// Log the raw event first
	eventJSON, _ := json.Marshal(event)
	log.Printf("Raw event received: %s", string(eventJSON))

	// Check if this looks like a direct webhook payload (has meta and content)
	var directPayload map[string]interface{}
	err := json.Unmarshal(eventJSON, &directPayload)
	if err == nil {
		// Check if it has the expected SignNow webhook structure
		if _, hasMeta := directPayload["meta"]; hasMeta {
			if _, hasContent := directPayload["content"]; hasContent {
				log.Printf("Using direct payload mode - detected SignNow webhook structure")
				return processWebhookPayload(directPayload)
			}
		}
	}

	// Try to parse as API Gateway event
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
	log.Printf("Headers: %v", apiGatewayEvent.Headers)
	log.Printf("Body length: %d", len(apiGatewayEvent.Body))
	log.Printf("Body: '%s'", apiGatewayEvent.Body)
	log.Printf("IsBase64Encoded: %v", apiGatewayEvent.IsBase64Encoded)

	// Check if body is empty
	if apiGatewayEvent.Body == "" {
		log.Printf("Error: Empty request body")
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       `{"error": "Empty request body"}`,
		}, nil
	}

	// Handle base64 encoded body (signatures)
	bodyData := apiGatewayEvent.Body
	if apiGatewayEvent.IsBase64Encoded {
		decoded, err := base64.StdEncoding.DecodeString(apiGatewayEvent.Body)
		if err != nil {
			log.Printf("Error decoding base64 body: %v", err)
			return LambdaResponse{
				StatusCode: 400,
				Headers:    map[string]string{"Content-Type": "application/json"},
				Body:       fmt.Sprintf(`{"error": "Invalid base64 encoding: %v"}`, err),
			}, nil
		}
		bodyData = string(decoded)
		log.Printf("Decoded base64 body: %s", bodyData)
	}

	// Parse the webhook payload from the event body
	var webhookPayload map[string]interface{}
	err = json.Unmarshal([]byte(bodyData), &webhookPayload)
	if err != nil {
		log.Printf("Error parsing webhook payload: %v", err)
		log.Printf("Raw body received: %s", bodyData)
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Invalid JSON payload: %v", "received_body": "%s"}`, err, bodyData),
		}, nil
	}

	return processWebhookPayload(webhookPayload)
}

// processWebhookPayload processes the webhook payload and creates the document.complete webhook
func processWebhookPayload(webhookPayload map[string]interface{}) (LambdaResponse, error) {
	// Extract document_id from the content section
	content, ok := webhookPayload["content"].(map[string]interface{})
	if !ok {
		log.Printf("Error: content section not found in webhook payload")
		log.Printf("Available keys: %v", getKeys(webhookPayload))
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Content section not found in webhook payload", "available_keys": %v}`, getKeys(webhookPayload)),
		}, nil
	}

	log.Printf("Content section found with keys: %v", getKeys(content))

	// Get the document ID from the content section
	var documentID string
	var found bool

	if documentID, found = content["documentId"].(string); found {
		log.Printf("Found documentId: %s", documentID)
	} else if documentID, found = content["document_id"].(string); found {
		log.Printf("Found document_id: %s", documentID)
	} else {
		log.Printf("Error: Neither documentId nor document_id found in content section")
		log.Printf("Content keys: %v", getKeys(content))
		return LambdaResponse{
			StatusCode: 400,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Document ID not found in webhook payload", "content_keys": %v}`, getKeys(content)),
		}, nil
	}

	log.Printf("Successfully extracted document ID: %s", documentID)

	// Get API token from environment variable
	apiToken := os.Getenv("SIGNNOW_API_TOKEN")
	if apiToken == "" {
		log.Printf("Error: SIGNNOW_API_TOKEN environment variable not set")
		return LambdaResponse{
			StatusCode: 500,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       `{"error": "API token not configured"}`,
		}, nil
	}

	// Get callback URL from environment variable
	callbackURL := os.Getenv("CALLBACK_URL")
	if callbackURL == "" {
		log.Printf("Error: CALLBACK_URL environment variable not set")
		return LambdaResponse{
			StatusCode: 500,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       `{"error": "Callback URL not configured"}`,
		}, nil
	}

	// Create the webhook for document.complete event
	webhookResponse, err := createDocumentCompleteWebhook(apiToken, documentID, callbackURL)
	if err != nil {
		log.Printf("Error creating webhook: %v", err)
		return LambdaResponse{
			StatusCode: 500,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf(`{"error": "Failed to create webhook: %v"}`, err),
		}, nil
	}

	log.Printf("Webhook created successfully: %s", webhookResponse)

	// Return success response with document ID and webhook response
	return LambdaResponse{
		StatusCode: 200,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       fmt.Sprintf(`{"message": "Webhook created successfully", "document_id": "%s", "webhook_response": "%s"}`, documentID, webhookResponse),
	}, nil
}

// getKeys is a helper function to extract keys from a map for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// main is the entry point of the application
func main() {
	// Check if running locally or in Lambda
	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		lambda.Start(handleRequest)
	} else {
		log.Println("Running locally - testing with test.json")
		documentID := testDocumentIDExtraction()
		createDocumentCompleteWebhook("54dabbcbcd73c76cb77d503c860d4d2cb588ada56357462ff1ae36c589421518", documentID, "http://apicallbacks.pdffiller.com/handle?hash=6e2a808b")
	}
}

// testDocumentIDExtraction reads the test.json file and extracts the document ID
func testDocumentIDExtraction() string {
	log.Println("=== Testing Document ID Extraction from test.json ===")

	// Read the test.json file from the current directory
	jsonData, err := os.ReadFile("test.json")
	if err != nil {
		log.Fatalf("Error reading test.json file: %v", err)
	}

	// Parse the JSON to get the document_id
	var webhookPayload map[string]interface{}
	err = json.Unmarshal(jsonData, &webhookPayload)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	// Extract document_id from the content section
	content, ok := webhookPayload["content"].(map[string]interface{})
	if !ok {
		log.Fatalf("Error: content section not found in JSON")
	}

	documentID, ok := content["documentId"].(string)
	if !ok {
		log.Fatalf("Error: documentId not found in content section")
	}

	log.Printf("Successfully extracted document ID: %s", documentID)

	return documentID
}

// createDocumentCompleteWebhook creates a webhook subscription for document.complete event
func createDocumentCompleteWebhook(API_ACCESS_TOKEN string, documentID string, callbackURL string) (string, error) {

	// SignNow API endpoint for creating webhook events
	url := "https://api.signnow.com/api/v2/events"

	// Create the JSON payload for the webhook creation request
	payload := strings.NewReader("{\n  \"event\": \"document.complete\",\n  \"entity_id\": \"" + documentID + "\",\n  \"action\": \"callback\",\n  \"attributes\": {\n    \"callback\": \"" + callbackURL + "\",\n    \"use_tls_12\": true,\n    \"docid_queryparam\": true,\n    \"headers\": {\n      \"string_head\": \"sample_text\",\n      \"int_head\": 12,\n      \"bool_head\": false,\n      \"float_head\": 12.24\n    }\n  }\n}")

	// Create HTTP POST request to the SignNow API
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	// Add required headers for the API request
	req.Header.Add("Authorization", "Bearer "+API_ACCESS_TOKEN)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	// Execute the HTTP request
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making request: %v", err)
	}

	// Ensure the response body is closed after reading
	defer res.Body.Close()

	// Read the response body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %v", err)
	}

	responseBody := string(body)

	// Check if the request was successful, should return 200
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		return responseBody, fmt.Errorf("webhook creation failed with status %d: %s", res.StatusCode, responseBody)
	}

	return responseBody, nil
}
