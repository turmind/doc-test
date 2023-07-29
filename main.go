package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	yaml "gopkg.in/yaml.v2"

	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
)

const (
	connectionStringTemplate = "mongodb://%s:%s@%s/sample-database?tls=true&replicaSet=rs0&readpreference=%s"
)

type Config struct {
	CAFilePath      string `yaml:"caFilePath"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	ClusterEndpoint string `yaml:"clusterEndpoint"`
	ReadPreference  string `yaml:"readPreference"`
	CollectionName  string `yaml:"collectionName"`
	ConnectTimeout  int    `yaml:"connectTimeout"`
	QueryTimeout    int    `yaml:"queryTimeout"`
	ThreadNumber    int    `yaml:"threadNumber"`
	InsertNumber    int    `yaml:"insertNumber"`
	JsonFileName    string `yaml:"jsonFileName"`
}

func main() {
	configFileName := flag.String("f", "config.yaml", "config file name")
	flag.Parse()
	config := Config{}
	// get configyaml from file config.yaml
	configYaml, err := ioutil.ReadFile(*configFileName)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(configYaml, &config)
	if err != nil {
		panic(err)
	}
	fmt.Println(config)
	jsonContent, err := ioutil.ReadFile(config.JsonFileName)
	if err != nil {
		panic(err)
	}
	var jsonData interface{}
	json.Unmarshal(jsonContent, &jsonData)

	connectionURI := fmt.Sprintf(connectionStringTemplate, config.Username, config.Password, config.ClusterEndpoint, config.ReadPreference)
	tlsConfig, err := getCustomTLSConfig(config.CAFilePath)
	if err != nil {
		log.Fatalf("Failed getting TLS configuration: %v", err)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < config.ThreadNumber; i++ {
		wg.Add(1)
		threadNO := i
		go func(threadNO int) {
			fmt.Println("run thead: ", threadNO)
			client, err := mongo.NewClient(options.Client().ApplyURI(connectionURI).SetTLSConfig(tlsConfig))
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.ConnectTimeout)*time.Second)
			defer cancel()
			err = client.Connect(ctx)
			if err != nil {
				log.Fatalf("Failed to connect to cluster: %v", err)
			}
			// Force a connection to verify our connection string
			err = client.Ping(ctx, nil)
			if err != nil {
				log.Fatalf("Failed to ping cluster: %v", err)
			}

			fmt.Println("Connected to DocumentDB!")

			collection := client.Database("test").Collection(config.CollectionName)

			for j := 0; j < config.InsertNumber; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.QueryTimeout)*time.Second)
				_, err := collection.InsertOne(ctx, jsonData)
				if err != nil {
					log.Fatalf("Failed to insert document: %v", err)
				}

				// id := res.InsertedID
				// log.Printf("Inserted document ID: %s", id)
				cancel()
			}
			wg.Done()
		}(threadNO)
	}
	wg.Wait()
	end := time.Now()
	fmt.Println("total time: ", end.Sub(start))
	/* query part
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(config.QueryTimeout)*time.Second)
	defer cancel()
	cur, err := collection.Find(ctx, bson.D{})

	if err != nil {
		log.Fatalf("Failed to run find query: %v", err)
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result bson.M
		err := cur.Decode(&result)
		log.Printf("Returned: %v", result)

		if err != nil {
			log.Fatal(err)
		}
	}

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	*/

}

func getCustomTLSConfig(caFile string) (*tls.Config, error) {
	tlsConfig := new(tls.Config)
	certs, err := ioutil.ReadFile(caFile)

	if err != nil {
		return tlsConfig, err
	}

	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs)

	if !ok {
		return tlsConfig, errors.New("Failed parsing pem file")
	}

	return tlsConfig, nil
}
