package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const VERSION = "1.0.0"

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type(
	Logger interface{
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct{
		mutex sync.Mutex
		mutexes map[string] *sync.Mutex
		dir string
		log Logger
	}
)

type Options struct{
	Logger
}

func New(dir string, options *Options)(*Driver, error){
	dir = filepath.Clean(dir)

	opts := Options{}

	if options != nil{
		opts = *options
	}

	if opts.Logger == nil{
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir: dir,
		mutexes: make(map[string]*sync.Mutex),
		log: opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil{
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection string, resource string, v interface{}) error{
	if collection == ""{
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	if resource == ""{
		return fmt.Errorf("Missing resource - unable to save record(no name)!")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil{
		return err
	}

	b ,err := json.MarshalIndent(v, "","\t")

	if err != nil{
		return err
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil{
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}




func (d *Driver) Read(collection string, resource string, v interface{}) error{
	if collection == ""{
		return fmt.Errorf("Missing collection - unable to read record!")
	}

	if resource == ""{
		return fmt.Errorf("Missing resource - unable to read record(no name)!")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil{
		return err
	}

	b, err := os.ReadFile(record +".json")
	if err != nil{
		return err
	}
	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string, )([]string, error){
	if collection ==""{
		return nil, fmt.Errorf("Missing collection - unable to read record!")
	}

	dir := filepath.Join(d.dir, collection)
	
	if _, err := stat(dir); err != nil{
		return nil, err
	}
	files, _ := os.ReadDir(dir)

	var records []string

	for _, file := range files{
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil{
			return nil, err
		}
		records = append(records, string(b))
	}
	return records, nil
}


func (d *Driver) Delete(collection string, resource string) error{
	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir);{
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find or directory name %v\n", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir +".json")
	}
	return nil
}



func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex{

	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok{
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string)(os.FileInfo, error){
	var fi os.FileInfo
	var err error
	if fi, err = os.Stat(path); os.IsNotExist(err){
		fi, err = os.Stat(path+".json")
	}
	return fi, err
}

type User struct{
	Name string
	Age json.Number
	Contact string
	Company string
	Address Address	
}

func main(){
	dir := "./"

	db, err := New(dir, nil)

	if err != nil{
		fmt.Println("Error ", err)
	}

	employees := []User{
		{"Ali","22","8768876","Google", Address{"New Delhi", "New Delhi", "India", "110019"}},
		{"Sawil","24","8004814729","Gartner", Address{"Lucknow", "Uttar Pradesh", "India", "226002"}},
		{"Yash","24","7637467","Jahaaz", Address{"Mumbai", "Maharashtra", "India", "656474"}},
		{"Wasil","26","988857","Salesforce", Address{"Hyderabad", "Telengana", "India", "765645"}},
		{"Karan","24","985664","Permify", Address{"Bangalore", "Karnataka", "India", "1986543"}},
		{"Houdin", "24","0000001","Lisadia Tech", Address{"New York City", "New York", "USA", "111111"}},
	}

	for _, value := range employees{
		db.Write("users", value.Name, User{
			Name: value.Name,
			Age: value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil{
		fmt.Println("error ", err)
	}
	fmt.Println(records)

	allusers := []User{}

	for _, emp := range records{
		employeeFound := User{}
		if err := json.Unmarshal([]byte(emp), &employeeFound); err != nil{
			fmt.Println("error ", err)
		}
		allusers = append(allusers, employeeFound)
	}

	fmt.Println(allusers)

	// if err := db.Delete("users", "Ali"); err != nil{
	// 	fmt.Println("Error ", err)
	// }

	// if err := db.Delete("users", ""); err != nil{
	// 	fmt.Println("Error ", err)
	// }
}