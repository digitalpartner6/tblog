package main

import(
    "fmt"
    "os"
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
        fmt.Println(path)

        Save2Mysql(path)

        return nil
    })
}

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
                  /*
                  if e.IsCreate() {
                      Flog("创建文件：%s", e.Name)
                  }else if e.IsDelete() {
                      Flog("删除文件：%s", e.Name)
                  }else if e.IsModify() {
                      Flog("修改文件：%s", e.Name)
                  }else if e.IsRename() {
                      Flog("重命名文件：%s", e.Name)
                      */
                  if e.IsAttrib() {
                      Save2Mysql(e.Name)
                      Flog("修改文件属性：%s", e.Name)
                  }
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
    f, err := os.Open(file)
    if err != nil {
        Flog("[ERRO] 文件读取失败:", err.Error())
        return
    }
    defer f.Close()

    bufreader := bufio.NewReader(f)

    count := 0
    for{
        line, err := bufreader.ReadString('\n')

        if err == io.EOF{
            Flog("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            break
        }
        count ++

        line = strings.Replace(line, "\r\n", "", -1)
        fmt.Println("读取行：", count)
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

        fmt.Println(info)

        err = M.SaveTbRecord(info)
        if err != nil {
            Flog("[ERRO] 写入数据库失败", err, info)
            return
            continue 
        }
        
        fmt.Println("SetStats")
        err = M.SetStats(fmt.Sprintf("%s_%s", info["FormulaName"], info["Symbol"]))
        if err != nil {
            Flog("[ERRO]:写入stat失败 ", err)
            return
        }

        fmt.Println(3)

//        Flog("[INFO]写入数据成功", info)
    }
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

