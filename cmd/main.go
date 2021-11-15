package main

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"log"
	"sync"
)

type request struct {
	CustomerId int `json:"customerId" validate:"required"`
	Id         int `json:"id" validate:"required"`
}

const (
	numberOfCustomers = 10
	numberOfRequests = 100
)

func main() {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String("eu-west-1")},
		Profile: "default",
	})

	if err != nil {
		log.Fatal(err)
	}

	s := sns.New(sess)

	var queueUrl = "arn:aws:sns:eu-west-1:236584826472:snsmain"

	var wg sync.WaitGroup
	wg.Add(numberOfCustomers)

	for i := 0 ; i < numberOfCustomers ; i++{
		go startSend(i,&wg,s,&queueUrl)
	}

	wg.Wait()

}

func startSend(customerId int,wg *sync.WaitGroup,s *sns.SNS,queueUrl *string){
	defer wg.Done()
	for i := 0 ; i < numberOfRequests ; i++{
		arr,_ := json.Marshal(request{
			CustomerId: customerId,
			Id:         i,
		})
		var body = string(arr)
		m := sns.PublishInput{
			Message: &body,
			TopicArn: queueUrl,
		}

		_,err := s.Publish(&m)
		if err != nil {
			log.Printf("err : %s",err)
		}

	}
}
