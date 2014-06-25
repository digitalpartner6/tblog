package main

import(
    "fmt"
    "os"
    "path"
    "path/filepath"
    "bufio"
    "io"
    "time"
    "strings"
    "github.com/howeyc/fsnotify"
    "runtime"
   M "github.com/weisd/tblog/models"
)

var (
eventTime   = make(map[string]int64)
buildPeriod time.Time
DirPath string
)

func init(){
    DirPath = M.Cfg.MustValue("base", "path")
}

func main(){
    exit := make(chan bool)

    //DirPath = "./TBlist"
    initData(DirPath)
    NewWatcher(DirPath)

    for {
      select{
        case <-exit:
            runtime.Goexit()
      }
    }

}

// 每次启动时，读一偏文件，入库
func initData(dirPath string){
    filepath.Walk(dirPath, func(path string, f os.FileInfo, err error) error {
        if f == nil {
            return err
        }

        if f.IsDir(){
            return nil
        }

        //path = strings.ToLower(path)
        if !strings.HasSuffix(path, "TXT"){
            return nil
        }
        //fmt.Println(path)

        Save2Mysql(path)

        return nil
    })
}

// 监控目录
func NewWatcher(path string){
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        fmt.Println("[ERRO] NewWatcher Failed")
        os.Exit(2)
    }


    go func(){
        for {
            select{
              case e := <-watcher.Event:
                  // Prevent duplicated builds.
				if buildPeriod.Add(1 * time.Second).After(time.Now()) {
					continue
				}
				buildPeriod = time.Now()

				mt := getFileModTime(e.Name)
				if t := eventTime[e.Name]; mt == t {
					Flog("[SKIP] # %s #\n", e.String())
                    continue
				}
                eventTime[e.Name] = mt
                
                Save2Mysql(e.Name)
              case err := <-watcher.Error:
                  Flog("err: %s", err.Error())
            }
        }
    }()

    err = watcher.Watch(path)
    if err != nil {
        Flog("err : fail to watch dir ", err)
        os.Exit(2)
    }


}

func Flog(msg string, args... interface{}){
    fmt.Println(msg, args)
}

func Save2Mysql(file string){

    if !strings.HasSuffix(file, "TXT"){
            return 
    }

    f, err := os.Open(file)
    if err != nil {
        Flog("[ERRO] 文件读取失败:", err.Error())
        return
    }
    defer f.Close()

    /*
     Flog("[INFO]:读取文件：", file)
    _, file = path.Split(filepath.ToSlash(file))
    fnames := strings.Split(path.Base(file), "#")
    if len(fnames) <3 {
        return
    }
    sname := strings.TrimLeft(fnames[0], "$")
    symbol := fnames[1]
    */

    bufreader := bufio.NewReader(f)

    count := 0
    for{
        line, err := bufreader.ReadString('\n')

        if err == io.EOF{
            break
        }

        line = strings.Replace(line, "\r\n", "", -1)

        info := make(map[string]string)

        kvs := strings.Split(line, ";")
        for i := 0; i< len(kvs); i++{
            kv := strings.Split(kvs[i], "=")
            if len(kv) != 2 {
                Flog("[ERRO] 行中键值对格式不正确")
                continue
            }

            info[kv[0]] = kv[1]
        }

        err = M.SaveTbRecord(info)
        if err != nil {
            Flog("[ERRO] 写入数据库失败", err, info)
            continue 
        }


        /*
        err = M.DoUpdateInfo(sname, symbol)
    if err != nil {
        Flog("[ERRO]:update info failed",err)
        continue
    }

    err = M.Save2Redis(sname, symbol)
    if err != nil {
        Flog("[ERRO]:save2redis failed!", err)
        continue
    }
    */



        // 保存成功
        count ++
    }

    // 没有添加记录
    if count == 0 {
        Flog("[INFO]:没有新记录被添加")
        return
    }

    Flog("[INFO]: 共写入数据条数：", count)
    // 存完 record 再计算stats
    // 从文件名中得到策略名称
    Flog("[INFO]:读取文件：", file)
    _, file = path.Split(filepath.ToSlash(file))
    fnames := strings.Split(path.Base(file), "#")
    if len(fnames) <3 {
        return
    }
    sname := strings.TrimLeft(fnames[0], "$")
    
    // 更新统计信息
    err = M.DoUpdateInfo(sname, fnames[1])
    if err != nil {
        Flog("[ERRO]:update info failed",err)
        return
    }

    // 更新info到redis
    err = M.Save2Redis(sname, fnames[1])
    if err != nil {
        Flog("[ERRO]:save2redis failed!", err)
        return
    }

    return
}

// getFileModTime retuens unix timestamp of `os.File.ModTime` by given path.
func getFileModTime(path string) int64 {
	path = strings.Replace(path, "\\", "/", -1)
	f, err := os.Open(path)
	if err != nil {
		Flog("[ERRO] Fail to open file[ %s ]\n", err)
		return time.Now().Unix()
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		Flog("[ERRO] Fail to get file information[ %s ]\n", err)
		return time.Now().Unix()
	}

	return fi.ModTime().Unix()
}

