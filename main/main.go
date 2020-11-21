package main

import (
	"bufio"
	"context"
	"github.com/pluralism/stravasignaturecalculator"
	"log"
	"os"
	"sync"
	"time"
)

type Env struct {
	db stravasignaturecalculator.StravaDatastore
}

func main() {
	db, err := stravasignaturecalculator.NewConnection("mongodb://localhost:27017/?replicaSet=my-mongo-set&connect=direct")
	if err != nil { log.Fatalln(err) }
	log.Println("Connected to database with success!")
	env := &Env{db}

	defer func() {
		log.Println("Closing database connection...")
		_ = stravasignaturecalculator.CloseConnection(db)
	}()

	calculator, err := stravasignaturecalculator.NewCalculator()
	if err != nil {
		log.Fatalln(err)
	}

	var wg sync.WaitGroup
	done := make(chan bool)

	go func() {
		defer wg.Done()

		for {
			select {
			case <- done:
				return
			default:
				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
				activities, err := env.db.GetActivitiesWithoutSignature(ctx, 20)
				m := make(map[int][]uint64)

				if err == nil {
					for _, activity := range activities {
						signature := calculator.GetSignature(activity.LatLng)
						m[activity.ID] = signature
					}

					keys := make([]int, 0, len(m))
					for k := range m {
						keys = append(keys, k)
					}

					log.Printf("Got %d activities. Activities IDs: %v", len(keys), keys)
					ctx, _ = context.WithTimeout(context.Background(), 10 * time.Second)
					err = env.db.SetActivitiesSignatures(ctx, m)
				}

				select {
					case <- done:
						return
					case <- time.After(5 * time.Second):
						break
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		input := bufio.NewScanner(os.Stdin)
		for {
			input.Scan()
			if input.Text() == "quit" {
				close(done)
				return
			}
		}
	}()

	wg.Wait()
}
