package main

import (
    "database/sql"
    "fmt"
    "github.com/imroc/req"
    _ "github.com/lib/pq"
    "math/rand"
    "os"
    "runtime"
    "strconv"
    "strings"
    "time"
)

const (
    defaultHost = "192.168.1.63"
    defaultPort = "30000"
)

func connStr() string {
    host := os.Getenv("DB_HOST")
    port := os.Getenv("DB_PORT")

    if len(host) == 0 || len(port) == 0 {
        return fmt.Sprintf("user=postgres dbname=youtube host=%s port=%s sslmode=disable", defaultHost, defaultPort)
    } else {
        return fmt.Sprintf("user=postgres dbname=youtube host=%s port=%s sslmode=disable", host, port)
    }
}

type Data struct {
    serial string
    subs   uint64
    videos uint64
    views  uint64
}

func (that Data) String() string {
    return fmt.Sprintf("{%s, %d, %d, %d}",
        that.serial, that.subs, that.views, that.videos)
}

func connection() *sql.DB {
    db, err := sql.Open("postgres", connStr())
    if err != nil {
        panic(err)
    }

    return db
}

func channels() []string {
    sqlStr := "SELECT DISTINCT SERIAL FROM youtube.entities.channels ORDER BY RANDOM() LIMIT 50"
    db := connection()
    defer func() {
        err := db.Close()
        if err != nil {
            panic(err)
        }
    }()

    row, err := db.Query(sqlStr)
    if err != nil {
        panic(err)
    }

    serials := make([]string, 50)
    var idx uint8
    for row.Next() {
        var serial string

        err = row.Scan(&serial)
        if err != nil {
            panic(err)
        }

        serials[idx] = serial
        idx++
    }

    return serials
}

func getKey() string {
    rawKey := os.Getenv("API_KEY")
    splitKeys := strings.Split(rawKey, "|")

    return splitKeys[rand.Intn(len(splitKeys))]
}

func getJson(cs []string) interface{} {
    key := getKey()
    url := "https://www.googleapis.com/youtube/v3/channels"
    partStr := "statistics"
    idStr := strings.Join(cs, ",")

    param := req.Param{
        "part":  partStr,
        "id": idStr,
        "key": key,
    }

    r, err := req.Get(url, param)
    if err != nil {
        panic(err)
    }

    var foo interface{}
    err = r.ToJSON(&foo)
    if err != nil {
        panic(err)
    }

    return foo
}

func getData(cs []string) []Data {
    jsonMap := getJson(cs).(map[string]interface{})
    items := jsonMap["items"].([]interface{})

    datas := make([]Data, len(cs))
    for i := range items {
        var data Data
        item := items[i].(map[string]interface{})
        {
            data.serial = item["id"].(string)
            {
                stats := item["statistics"].(map[string]interface{})
                subs, err := strconv.ParseUint(stats["subscriberCount"].(string), 10, 64)
                if err != nil {
                    panic(err)
                }
                data.subs = subs

                vids, err := strconv.ParseUint(stats["videoCount"].(string), 10, 64)
                if err != nil {
                    panic(err)
                }
                data.videos = vids

                views, err := strconv.ParseUint(stats["viewCount"].(string), 10, 64)
                if err != nil {
                    panic(err)
                }
                data.views = views
            }
        }

        fmt.Println(data)
        datas[i] = data
    }

    return datas
}

func insert(ds []Data) {
    db := connection()
    defer func() {
        err := db.Close()
        if err != nil {
            panic(err)
        }
    }()

    sqlInsert := "INSERT INTO youtube.entities.chan_stats (serial, subs, videos, views) VALUES ($1, $2, $3, $4)"

    for i := range ds {
        d := ds[i]

        _, err := db.Exec(sqlInsert, d.serial, d.subs, d.videos, d.views)
        if err != nil {
            panic(err)
        }
    }
}

func main() {
    rand.Seed(time.Now().Unix())
    for {
        chans := channels()
        datas := getData(chans)
        insert(datas)

        runtime.GC()
    }
}
