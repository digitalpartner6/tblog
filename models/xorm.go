package models

import(
    "fmt"
    "path"
    "path/filepath"
    "os"
    "os/exec"
    "strings"
    "github.com/Unknwon/goconfig"
   _ "github.com/go-sql-driver/mysql"
   "github.com/go-xorm/xorm"
)

var Engine *xorm.Engine
var Cfg *goconfig.ConfigFile

// 执行文件所在目录
func ExecDir() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	return path.Dir(strings.Replace(p, "\\", "/", -1)), nil
}

func init(){
    workDir , err := ExecDir()
    if err != nil {
        fmt.Println(err)
        return 
    }

    cfgPath := filepath.Join(workDir, "conf.ini")
    Cfg, err = goconfig.LoadConfigFile(cfgPath)

    if err != nil {
        fmt.Println(err)
        return 
    }

    user := Cfg.MustValue("mysql", "user")
    pwd := Cfg.MustValue("mysql", "passwd")
    host := Cfg.MustValue("mysql", "host")
    port := Cfg.MustValue("mysql", "port")
    dbname := Cfg.MustValue("mysql", "dbname")
    charset := Cfg.MustValue("mysql", "charset")
    
    dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, pwd, host, port, dbname, charset)

    Engine, err = xorm.NewEngine("mysql", dns)
    if err != nil {
        fmt.Println(err)
        os.Exit(2)
    }

    err = Engine.Sync(new(TbRecord))
    if err != nil {
        fmt.Println(err)
        os.Exit(2)
    }

}
