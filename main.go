package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net/http"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "bulkinsert"
)

type Request struct {
	RequestID int           `json:"request_Id"`
	Data      []Transaction `json:"data"`
}
type Transaction struct {
	Id        int     `json:"id"`
	Customer  string  `json:"customer"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
	Timestamp string  `json:"timestamp"`
}

func main() {
	// Open a connection to the PostgreSQL database
	db := openDB()
	defer db.Close()

	query, err := db.Prepare("INSERT INTO transaction (customer, quantity, price, timestamp) VALUES ($1, $2, $3, $4)")
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	// Create an HTTP server with an endpoint to insert data into the database
	http.HandleFunc("/bulkinsert", func(w http.ResponseWriter, r *http.Request) {
		// Parse the request body into a list of users
		var req Request
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Begin a transaction
		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if err != nil {
				tx.Rollback()
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}()

		for _, d := range req.Data {
			_, err := query.Exec(d.Customer, d.Quantity, d.Price, d.Timestamp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// Commit the transaction
		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Return a success response to the client
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Data inserted successfully!")
	})

	// Start the HTTP server
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func openDB() *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
