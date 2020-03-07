package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/d1str0/hpfeeds"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "output lots of stuff")
	flag.Parse()

	c := createClient()
	b := &hpfeeds.Broker{
		Name: "mhn",
		Port: 10000,
		DB:   c,
	}

	if verbose {
		b.SetDebugLogger(log.Print)
		b.SetInfoLogger(log.Print)
		b.SetErrorLogger(log.Print)
	}

	err := b.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

type mongoClient struct {
	*mongo.Client
}

func createClient() *mongoClient {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	return &mongoClient{client}
}

// Identify implements the hpfeeds.Identifier interface.
func (c *mongoClient) Identify(ident string) (*hpfeeds.Identity, error) {
	// Hardcoded for MHN installs
	collection := c.Database("hpfeeds").Collection("auth_key")

	var result hpfeeds.Identity
	filter := bson.M{"ident": ident}
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	err := collection.FindOne(ctx, filter).Decode(&result)
	return &result, err
}
