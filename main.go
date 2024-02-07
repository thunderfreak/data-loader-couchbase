package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

var wg sync.WaitGroup

type User struct {
	Employee Employee `json:"employee"`
}
type Employee struct {
	Name           string         `json:"name"`
	Age            int64          `json:"age"`
	ContactDetails ContactDetails `json:"contactDetails"`
}
type ContactDetails struct {
	Position string `json:"position"`
	Address  string `json:"address"`
	Skills   Skills `json:"skills"`
}
type Skills struct {
	PrimaryLanguage      string               `json:"primaryLanguage"`
	SecondaryLanguage    string               `json:"secondaryLanguage"`
	ProgrammingLanguages ProgrammingLanguages `json:"programmingLanguages"`
}
type ProgrammingLanguages struct {
	Preferred       string          `json:"preferred"`
	Familiar        []string        `json:"familiar"`
	Specializations Specializations `json:"specializations"`
}
type Specializations struct {
	Backend                string                 `json:"backend"`
	Frontend               string                 `json:"frontend"`
	DevelopmentMethodology DevelopmentMethodology `json:"developmentMethodology"`
}
type DevelopmentMethodology struct {
	Preferred        string           `json:"preferred"`
	Framework        string           `json:"framework"`
	Containerization Containerization `json:"containerization"`
}
type Containerization struct {
	Docker         string         `json:"docker"`
	Orchestration  string         `json:"orchestration"`
	CloudProviders CloudProviders `json:"cloudProviders"`
}
type CloudProviders struct {
	Preferred      string         `json:"preferred"`
	Alternative    []string       `json:"alternative"`
	Infrastructure Infrastructure `json:"infrastructure"`
}
type Infrastructure struct {
	Databases  []string `json:"databases"`
	Networking string   `json:"networking"`
}

func operateOnDoc(col *gocb.Collection, scope *gocb.Scope, bucket *gocb.Bucket) {
	noOfDocs := 50000
	var id string

	// Upsert document
	for i := 0; i < noOfDocs; i++ {
		// Create and store a Document
		id = uuid.NewString()
		_, err := col.Upsert(id,
			User{
				Employee: Employee{
					Name: "Soham Bhattacharjee",
					Age:  22,
					ContactDetails: ContactDetails{
						Position: "Cloud Engineer",
						Address:  "Couchbase India Pvt Ltd Block No.21, UB City, 11th floor, Concorde, Vittal Mallya Rd, KG Halli, Shanthala Nagar, Sampangirama Nagar, Bengaluru, Karnataka",
						Skills: Skills{
							PrimaryLanguage:   "C++",
							SecondaryLanguage: "JavaScript",
							ProgrammingLanguages: ProgrammingLanguages{
								Preferred: "Go",
								Familiar:  []string{"C++", "C", "Python", "Javascript", "Java", "Pascal", "Assembly", "Ruby", "R", "Rust", "Scala", "Typescript", "Script", "SQL"},
								Specializations: Specializations{
									Backend:  "Go",
									Frontend: "React",
									DevelopmentMethodology: DevelopmentMethodology{
										Preferred: "Agile",
										Framework: "Scrum",
										Containerization: Containerization{
											Docker:        "Docker",
											Orchestration: "Kubernetes",
											CloudProviders: CloudProviders{
												Preferred:   "AWS",
												Alternative: []string{"Azure", "GCP", "DigitalOcean", "IBM", "Oracle", "SalesForce", "AlibabaCloud", "LiquidWeb"},
												Infrastructure: Infrastructure{
													Databases:  []string{"Couchbase", "MySQL", "PostgreSQL", "MongoDB", "IBM"},
													Networking: "Networking",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}, nil)
		if err != nil {
			fmt.Println("Error while upsert: ", err, id)
		} else {
			fmt.Printf("Document: %v added \n", id)
		}
	}

	// // Read document
	// for {
	// 	getResult, err := col.Get(id, nil)
	// 	if err != nil {
	// 		fmt.Println("Error while getting document", err, id)
	// 	}

	// 	var readUser User
	// 	err = getResult.Content(&readUser)
	// 	if err != nil {
	// 		fmt.Println("Error while getting result", err, id)
	// 	}
	// 	// fmt.Printf("User: %v of document %v\n", readUser.Employee.Name, id)
	// }
	wg.Done()
}

func main() {

	// Cluster details
	connectionString := "localhost"
	bucketName := "b1"
	username := "Administrator"
	password := "123456"
	scopeName := "s1"
	collectionName := "c1"

	// Connecting to cluster
	cluster, err := gocb.Connect("couchbase://"+connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		fmt.Println("Error while creating connection: ", err)
	}

	bucket := cluster.Bucket(bucketName)
	scope := bucket.Scope(scopeName)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		fmt.Println("Error while wait for bucket: ", err)
	}

	// Reference to collection
	col := bucket.Scope(scopeName).Collection(collectionName)
	noOfRoutines := 1000
	for i := 0; i < noOfRoutines; i++ {
		wg.Add(1)
		go operateOnDoc(col, scope, bucket)
	}
	wg.Wait()
	// time.Sleep(2*time.Hour)
	fmt.Println("End of program!")
}
