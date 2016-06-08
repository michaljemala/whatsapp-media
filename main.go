package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	dbPath := flag.String("db", "", "A path to the SQLite DB file")
	mediaPath := flag.String("media", "", "A path to the WhatsApp media folder")
	targetPath := flag.String("target", "", "A path to the target media folder")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("Provide the path to the SQLite db file")
	}
	if *mediaPath == "" {
		log.Fatal("Provide the path to the WhatsApp media folder")
	}
	if _, err := os.Stat(*mediaPath); err != nil && os.IsNotExist(err) {
		log.Fatalf("%v does not exists", &mediaPath)
	}

	if *targetPath == "" {
		log.Fatal("Provide the path to the target media folder")
	}
	if _, err := os.Stat(*targetPath); err != nil && os.IsNotExist(err) {
		err := os.MkdirAll(*targetPath, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}

	db, err := sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	chatsMap := map[string]string{}

	chats, err := db.Query("select ZPARTNERNAME as Name, ZCONTACTJID as Id from ZWACHATSESSION")
	if err != nil {
		log.Fatal(err)
	}
	for chats.Next() {
		var (
			name string
			id   string
		)
		if err := chats.Scan(&name, &id); err != nil {
			log.Print(err)
			continue
		}
		if _, found := chatsMap[id]; found {
			log.Fatalf("Duplicate ID found: %v", id)
		}
		chatsMap[id] = name

		folder := fmt.Sprintf("%v/%v", *targetPath, name)
		if _, err := os.Stat(folder); err != nil && os.IsNotExist(err) {
			err := os.MkdirAll(folder, os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}
		}

	}
	err = chats.Err()
	if err != nil {
		log.Fatal(err)
	}

	items, err := db.Query("select A.ZMEDIASECTIONID, A.ZMESSAGEDATE, B.ZMEDIALOCALPATH from ZWAMESSAGE as A inner join ZWAMEDIAITEM as B on A.Z_PK=B.ZMESSAGE where B.ZMEDIALOCALPATH != '' and A.ZMEDIASECTIONID != '' order by A.ZMEDIASECTIONID")
	if err != nil {
		log.Fatal(err)
	}
	for items.Next() {
		var (
			id   string
			val  interface{}
			path string
		)
		if err := items.Scan(&id, &val, &path); err != nil {
			log.Fatal(err)
		}

		var datetime time.Time
		switch t := val.(type) {
		case time.Time:
			datetime = t
		case float64:
			sec := int64(t)
			nsec := t - float64(sec)
			datetime = time.Unix(sec, int64(nsec*1000000000))
		default:
			log.Fatal("Unsupported date time")
		}
		datetime = datetime.Add(978307200 * time.Second)

		path = strings.TrimLeft(path, "/")
		parts := strings.Split(path, "/")
		if len(parts) < 2 {
			log.Fatalf("Invalid media path: %v\n", path)
		}
		folder, found := chatsMap[parts[1]]
		if !found {
			log.Fatalf("Chat [%v] not found: %v\n%v", parts[1], path, parts)
		}

		extParts := strings.Split(parts[len(parts)-1], ".")
		if len(extParts) != 2 {
			log.Fatalf("Invalid file name: %v\n", path)
		}
		extension := extParts[1]

		filename := fmt.Sprintf("%v.%v", datetime.Format("20060102150405.000"), extension)

		src := fmt.Sprintf("%v/%v", *mediaPath, strings.Join(parts[1:], "/"))
		dst := fmt.Sprintf("%v/%v/%v", *targetPath, folder, filename)

		err := copy(src, dst)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = items.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func copy(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		return fmt.Errorf("copy: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("copy: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}

	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}
