package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"encoding/json"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Vote struct {
	VoterID string `json:"voter_id"`
	Vote    string `json:"vote"`
}

func main() {
	db := openDbConnection()
	redisClient := openRedisConnection()
	defer redisClient.Close()

	for {
		time.Sleep(100 * time.Millisecond)
		vote, _ := redisClient.LPop(ctx, "votes").Result()

		//process vote
		if vote != "" {
			//parse vote
			var v Vote
			json.Unmarshal([]byte(vote), &v)

			fmt.Printf("Processing vote for '%s' by '%s'\n", v.Vote, v.VoterID)
			//update vote to db
			//check if db is connected
			if db.Ping() != nil {
				db = openDbConnection()
			}

			updateVote(db, v.VoterID, v.Vote)
		}

	}

}

func updateVote(db *sql.DB, voterID string, vote string) {
	_, err := db.Exec("INSERT INTO votes (id, vote) VALUES ($1, $2)", voterID, vote)
	if err != nil {
		_, err := db.Exec("UPDATE votes SET vote = $1 WHERE id = $2", vote, voterID)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("Processed vote for '%s' by '%s'\n", vote, voterID)
}

func openDbConnection() *sql.DB {
	var db *sql.DB
	conn, err := pq.NewConnector("postgres://postgres:postgres@db:5432/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}

	for {
		db = sql.OpenDB(conn)
		if err := db.Ping(); err != nil {
			fmt.Println("DB not ready")
		} else {
			fmt.Println("DB connected")
			break
		}
		time.Sleep(1 * time.Second)
	}

	// check if table exists
	_, err = db.Query("SELECT * FROM votes")
	if err != nil {
		// create table
		_, err := db.Exec("CREATE TABLE votes (id VARCHAR(255) NOT NULL UNIQUE, vote VARCHAR(255) NOT NULL)")
		if err != nil {
			panic(err)
		}
	}

	return db
}

func openRedisConnection() *redis.Client {
	var rdb *redis.Client

	for {
		rdb = redis.NewClient(&redis.Options{
			Addr:     "redis:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		pong, err := rdb.Ping(ctx).Result()
		if err != nil {
			fmt.Println("Redis not ready")
		} else {
			fmt.Println(pong)
			break
		}
	}
	return rdb
}
